import json
import os
import shutil
import tempfile
import threading
from pathlib import Path

import pytest

from memo.recording.metadata import DirectorySession, SessionOrigin, SnapshotEntry, StepManifest
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionNotFoundError, SessionStore


def _paths(tmp_path: Path) -> StoragePaths:
    return StoragePaths(tmp_path)


def _session(root: Path) -> DirectorySession:
    return DirectorySession(
        "session", str(root.resolve()), "now", "now", SessionOrigin("1.0.0", "user", "host")
    )


def _publish(
    store: SessionStore,
    session: DirectorySession,
    step: int,
    stream_high_water: dict[str, int] | None = None,
) -> StepManifest:
    temporary = Path(tempfile.mkdtemp(prefix="prepared-", dir=store.session_path("session")))
    (temporary / "file.txt").write_text(str(step))
    return store.publish(
        session,
        StepManifest(
            "session",
            step,
            "now",
            f"snapshots/{step}",
            [SnapshotEntry("file.txt", "file", 0o644, 1)],
            stream_high_water=dict(stream_high_water or {}),
        ),
        temporary,
    )


TERMINAL = "terminal"
CHUNK_NAMES = [f"chunks/{index:08d}-{index:08d}.jsonl.gz" for index in range(5)]


def _recorded_session(tmp_path: Path, steps: int) -> SessionStore:
    """Publish ``steps`` steps that all reference one multi-chunk terminal stream."""
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    directory = store.create(session)
    terminal_path = directory / "streams" / "terminals" / TERMINAL
    (terminal_path / "chunks").mkdir(parents=True)
    for name in CHUNK_NAMES:
        (terminal_path / name).write_bytes(b"")
    (terminal_path / "stream.json").write_text(
        json.dumps({"highest_sequence": 50, "chunks": CHUNK_NAMES})
    )
    for step in range(steps):
        _publish(store, session, step, stream_high_water={TERMINAL: step + 1})
    return store


def test_history_validation_probes_each_stream_chunk_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    steps = 8
    store = _recorded_session(tmp_path, steps)
    original = Path.is_file
    probed: list[str] = []

    def counting_is_file(self: Path) -> bool:
        if self.name.endswith(".jsonl.gz"):
            probed.append(self.name)
        return original(self)

    monkeypatch.setattr(Path, "is_file", counting_is_file)

    assert len(store.steps("session")) == steps

    # Every step references the same chunk list, so validating per step would
    # probe each chunk once per step instead of once per recording.
    assert sorted(probed) == sorted(Path(name).name for name in CHUNK_NAMES)


def test_history_validation_still_rejects_a_missing_stream_chunk(tmp_path: Path) -> None:
    store = _recorded_session(tmp_path, 4)
    chunk = store.session_path("session") / "streams" / "terminals" / TERMINAL / CHUNK_NAMES[2]
    chunk.unlink()
    with pytest.raises(ValueError, match="missing chunk"):
        store.steps("session")


def test_history_validation_still_rejects_a_stream_short_of_a_step(tmp_path: Path) -> None:
    store = _recorded_session(tmp_path, 4)
    metadata = store.session_path("session") / "streams" / "terminals" / TERMINAL / "stream.json"
    metadata.write_text(json.dumps({"highest_sequence": 2, "chunks": CHUNK_NAMES}))
    with pytest.raises(ValueError, match="does not reach step"):
        store.steps("session")


def test_history_validation_still_rejects_a_missing_snapshot(tmp_path: Path) -> None:
    store = _recorded_session(tmp_path, 3)
    shutil.rmtree(store.session_path("session") / "snapshots" / "1")
    with pytest.raises(ValueError, match="missing snapshot"):
        store.steps("session")


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
    (path / "session.json").write_text(
        json.dumps(
            {
                "session_id": "old",
                "root": str(tmp_path.resolve()),
                "origin": {"memo_version_id": "1.0.0", "username": "user", "hostname": "host"},
                "created_utc": "now",
                "updated_utc": "now",
                "format": "memo-directory-session",
                "format_version": 1,
            }
        )
    )
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
    from memo.recording import store as session_store

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


def test_publish_recovers_orphan_snapshot_for_next_step(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    directory = store.create(session)
    _publish(store, session, 0)
    orphan = directory / "snapshots" / "1"
    orphan.mkdir()
    (orphan / "stale.txt").write_text("stale")

    published = _publish(store, session, 1)

    assert published.step == 1
    assert (directory / "HEAD").read_text() == "1\n"
    assert not (directory / "snapshots" / "1" / "stale.txt").exists()
    assert (directory / "snapshots" / "1" / "file.txt").read_text() == "1"


def test_publish_recovers_valid_step_with_stale_head(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    directory = store.create(session)
    first = _publish(store, session, 0)
    from memo.recording import store as session_store

    original = session_store.atomic_write

    def fail_head(path: Path, data: bytes) -> None:
        if path.name == "HEAD":
            raise OSError("injected publication failure")
        original(path, data)

    monkeypatch.setattr(session_store, "atomic_write", fail_head)
    with pytest.raises(OSError, match="injected"):
        _publish(store, session, 1)
    assert store.head("session") == first

    monkeypatch.setattr(session_store, "atomic_write", original)
    repaired = _publish(store, session, 1)

    assert repaired.step == 1
    assert (directory / "HEAD").read_text() == "1\n"
    assert store.head("session").step == 1


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


def test_remove_archived_requires_complete_fully_pushed_head(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path / "home"))
    session = _session(root)
    directory = store.create(session)
    head = _publish(store, session, 0)

    with pytest.raises(ValueError, match="not complete"):
        store.remove_archived("session")
    assert directory.is_dir()

    session.state = "complete"
    store.update_session(session)
    with pytest.raises(ValueError, match="not archived"):
        store.remove_archived("session")
    assert directory.is_dir()

    session.last_pushed_step = head.step
    session.last_pushed_digest = "invalid"
    session.remote_object = "sessions/session/generations/00000000.tar.zst"
    store.update_session(session)
    with pytest.raises(ValueError, match="metadata is incomplete"):
        store.remove_archived("session")
    assert directory.is_dir()

    session.last_pushed_digest = "0" * 64
    store.update_session(session)
    store.remove_archived("session")

    assert not directory.exists()


def _archived(tmp_path: Path) -> tuple[SessionStore, Path]:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path / "home"))
    session = _session(root)
    directory = store.create(session)
    head = _publish(store, session, 0)
    session.state = "complete"
    session.last_pushed_step = head.step
    session.last_pushed_digest = "0" * 64
    session.remote_object = "remote"
    store.update_session(session)
    return store, directory


def test_remove_archived_validates_history_before_deleting(tmp_path: Path) -> None:
    store, directory = _archived(tmp_path)
    store.remove_archived("session")
    assert not directory.exists()


def test_remove_archived_keeps_a_recording_whose_snapshot_is_gone(tmp_path: Path) -> None:
    store, directory = _archived(tmp_path)
    # What was uploaded is a copy of this tree, so a snapshot missing here is
    # missing in the archive too. Deleting on the strength of the push record
    # alone is how filesystem history gets lost for good.
    shutil.rmtree(directory / "snapshots" / "0")
    with pytest.raises(ValueError, match="missing snapshot"):
        store.remove_archived("session")
    assert directory.exists()


def test_remove_archived_renames_before_recursive_removal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path / "home"))
    session = _session(root)
    directory = store.create(session)
    head = _publish(store, session, 0)
    session.state = "complete"
    session.last_pushed_step = head.step
    session.last_pushed_digest = "0" * 64
    session.remote_object = "remote"
    store.update_session(session)
    destinations: list[Path] = []

    def fail_removal(path: Path) -> None:
        destinations.append(path)
        raise OSError("injected removal failure")

    monkeypatch.setattr("memo.recording.store.shutil.rmtree", fail_removal)
    with pytest.raises(OSError, match="injected"):
        store.remove_archived("session")

    assert not directory.exists()
    assert len(destinations) == 1
    assert destinations[0].parent == store.paths.archive / ".removing"
    assert destinations[0].is_dir()


def test_amend_session_keeps_a_concurrent_writer_s_fields(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    store.create(_session(root))

    # The archive publisher records where the upload landed...
    store.amend_session(
        "session",
        last_pushed_step=7,
        last_pushed_digest="0" * 64,
        remote_object="remote",
    )
    # ...while the lifecycle writer, holding a copy loaded beforehand, completes.
    store.amend_session("session", state="complete", updated_utc="later")

    current = store.load_session("session")
    assert current.state == "complete"
    assert current.last_pushed_step == 7
    assert current.remote_object == "remote"


def test_update_session_reverts_a_concurrent_writer_s_fields(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    store.create(_session(root))
    stale = store.load_session("session")

    store.amend_session(
        "session",
        last_pushed_step=7,
        last_pushed_digest="0" * 64,
        remote_object="remote",
    )
    stale.state = "complete"
    store.update_session(stale)

    # This is the hazard amend_session exists to avoid: the recording now claims
    # to be complete while pointing at no uploaded generation at all.
    assert store.load_session("session").last_pushed_step is None


def test_amend_session_rejects_unknown_fields(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    store.create(_session(root))
    with pytest.raises(AttributeError, match="unknown directory session fields"):
        store.amend_session("session", nonsense=1)


def test_amend_session_serialises_concurrent_writers(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    store.create(_session(root))
    start = threading.Barrier(3)

    def amend(**changes: object) -> None:
        start.wait(timeout=5)
        for _ in range(20):
            store.amend_session("session", **changes)

    threads = [
        threading.Thread(target=amend, kwargs={"state": "ending"}),
        threading.Thread(target=amend, kwargs={"capture_scope": "full"}),
    ]
    for thread in threads:
        thread.start()
    start.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()

    current = store.load_session("session")
    assert current.state == "ending"
    assert current.capture_scope == "full"


def test_resolving_head_does_not_probe_every_stream_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _recorded_session(tmp_path, 4)
    original = Path.is_file
    probed: list[str] = []

    def counting_is_file(self: Path) -> bool:
        if self.name.endswith(".jsonl.gz"):
            probed.append(self.name)
        return original(self)

    monkeypatch.setattr(Path, "is_file", counting_is_file)
    assert store.head("session") is not None

    # Resolving a step happens on every publish. Confirming the whole chunk
    # list still exists is an archive sweep and belongs in the integrity pass.
    assert probed == []


def test_resolving_head_still_rejects_a_stream_short_of_a_step(tmp_path: Path) -> None:
    store = _recorded_session(tmp_path, 4)
    metadata = store.session_path("session") / "streams" / "terminals" / TERMINAL / "stream.json"
    metadata.write_text(json.dumps({"highest_sequence": 1, "chunks": CHUNK_NAMES}))
    with pytest.raises(ValueError, match="does not reach step"):
        store.head("session")


def test_stream_metadata_cache_notices_a_rewritten_stream(tmp_path: Path) -> None:
    store = _recorded_session(tmp_path, 4)
    assert store.head("session") is not None
    metadata = store.session_path("session") / "streams" / "terminals" / TERMINAL / "stream.json"
    # A sealed stream grows while the recording runs, so a stale parse would
    # keep validating against a sequence the stream has long since passed.
    metadata.write_text(json.dumps({"highest_sequence": 1, "chunks": CHUNK_NAMES}))
    os.utime(metadata, (0, 0))
    with pytest.raises(ValueError, match="does not reach step"):
        store.head("session")
