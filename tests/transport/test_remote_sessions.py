from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import os
import tarfile
import tempfile
import threading
import tracemalloc
from pathlib import Path
from types import SimpleNamespace

import pytest
import zstandard

from memo.agents.run_metadata import AgentRunMetadata
from memo.export import replay_session
from memo.recording.filesystem import atomic_write
from memo.recording.git_snapshots import GitSnapshotError
from memo.recording.metadata import DirectorySession, SessionOrigin, SnapshotEntry, StepManifest
from memo.recording.paths import StoragePaths
from memo.recording.snapshots import StepPublisher
from memo.recording.store import SessionStore
from memo.recording.streams import StreamEvent
from memo.transport import (
    ensure_local_session,
    inspect_archived_agent_runs,
    list_archived_session_ids,
    prepare_generation,
    pull_session,
    push_session,
    remote_sessions,
)
from memo.transport.archive import atomic_install_directory, safe_extract_tar_zst_stream
from memo.transport.config import S3Config
from memo.transport.s3 import MULTIPART_PART_SIZE


class TrackingBody(io.BytesIO):
    def __init__(self, data: bytes, max_chunk: int = 4096) -> None:
        super().__init__(data)
        self.max_chunk = max_chunk
        self.read_sizes: list[int] = []
        self.was_closed = False
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            raise AssertionError("unbounded response body read")
        self.read_sizes.append(size)
        value = super().read(min(size, self.max_chunk))
        self.bytes_read += len(value)
        return value

    def close(self) -> None:
        self.was_closed = True
        super().close()


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.operations: list[tuple[str, str]] = []
        self.fail_key: str | None = None
        self.fail_operation: tuple[str, object] | None = None
        self.response_bodies: list[tuple[str, TrackingBody]] = []
        self.part_sizes: list[int] = []
        self.upload_options: dict[str, object] = {}

    @staticmethod
    def _bytes(value) -> bytes:
        return value.read() if hasattr(value, "read") else bytes(value)

    def put_object(
        self, bucket: str, key: str, data, length: int, content_type: str | None = None
    ) -> None:
        self.operations.append(("put", key))
        if key == self.fail_key or self.fail_operation == ("put", key):
            raise OSError("injected upload failure")
        value = data.read(length + 1)
        assert len(value) == length
        self.objects[key] = value

    def fput_object(self, bucket: str, key: str, file_path: str, **kwargs) -> None:
        self.operations.append(("upload", key))
        self.upload_options = kwargs
        if key == self.fail_key or self.fail_operation == ("upload", None):
            raise OSError("injected upload failure")
        chunks = []
        progress = kwargs.get("progress")
        total = Path(file_path).stat().st_size
        if progress is not None:
            progress.set_meta(key, total)
        completed = 0
        with Path(file_path).open("rb") as handle:
            while chunk := handle.read(MULTIPART_PART_SIZE):
                self.part_sizes.append(len(chunk))
                chunks.append(chunk)
                completed += len(chunk)
                if progress is not None:
                    progress.update(len(chunk))
        self.objects[key] = b"".join(chunks)

    def stat_object(self, bucket: str, key: str) -> object:
        self.operations.append(("stat", key))
        if key not in self.objects:
            raise KeyError(key)
        return SimpleNamespace(size=len(self.objects[key]))

    def get_object(self, bucket: str, key: str) -> TrackingBody:
        self.operations.append(("get", key))
        body = TrackingBody(self.objects[key])
        self.response_bodies.append((key, body))
        return body

    def list_objects(self, bucket: str, prefix: str, recursive: bool = True):
        self.operations.append(("list", prefix))
        return (
            SimpleNamespace(object_name=key)
            for key in sorted(self.objects)
            if key.startswith(prefix)
        )


def _paths(root: Path) -> StoragePaths:
    return StoragePaths(root)


def _git_session(paths: StoragePaths, root: Path) -> tuple[SessionStore, DirectorySession]:
    root.mkdir()
    (root / "note.txt").write_text("captured")
    store = SessionStore(paths)
    session = DirectorySession(
        "git-session", str(root.resolve()), "now", "now", SessionOrigin("1.0.0", "user", "host")
    )
    store.create(session)
    StepPublisher(store).publish(session)
    session.state = "complete"
    store.update_session(session)
    return store, session


def test_list_archived_session_ids_uses_index_and_filters_invalid_keys() -> None:
    client = FakeS3()
    digest = "a" * 64
    client.objects.update(
        {
            f"prefix/index/sessions/b/{digest}.json": b"{}",
            f"prefix/index/sessions/a/{digest}.json": b"{}",
            "prefix/index/sessions/nested/session.json": b"{}",
            "prefix/index/sessions/readme.txt": b"",
            "prefix/unrelated.json": b"{}",
        }
    )

    assert list_archived_session_ids(S3Config("bucket", "prefix"), client) == ["a", "b"]


def test_pull_all_skips_local_sessions_and_continues_after_failures(
    tmp_path: Path, monkeypatch
) -> None:
    paths = _paths(tmp_path / "home")
    (paths.archive / "local").mkdir(parents=True)
    config = S3Config("bucket", "prefix")
    remote = object()
    calls: list[tuple[str, bool, object]] = []

    monkeypatch.setattr(remote_sessions, "_store", lambda _config, _client: remote)
    monkeypatch.setattr(
        remote_sessions,
        "list_archived_session_ids",
        lambda _config, _client: ["failed", "local", "new"],
    )

    def fake_pull(session_id, _paths, _config, force=False, client=None):
        calls.append((session_id, force, client))
        if session_id == "failed":
            raise OSError("offline")
        return _paths.archive / session_id

    monkeypatch.setattr(remote_sessions, "pull_session", fake_pull)

    summary = remote_sessions.pull_all_sessions(paths, config)

    assert summary.pulled == ["new"]
    assert summary.skipped == ["local"]
    assert summary.failed == [("failed", "offline")]
    assert sorted(calls) == [("failed", False, remote), ("new", False, remote)]

    calls.clear()
    forced = remote_sessions.pull_all_sessions(paths, config, force=True)
    assert forced.pulled == ["local", "new"]
    assert forced.skipped == []
    assert forced.failed == [("failed", "offline")]
    assert sorted(calls) == [
        ("failed", True, remote),
        ("local", True, remote),
        ("new", True, remote),
    ]


def test_pull_all_pulls_sessions_concurrently(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path / "home")
    config = S3Config("bucket", "prefix")
    barrier = threading.Barrier(2)

    monkeypatch.setattr(remote_sessions, "_store", lambda _config, _client: object())
    monkeypatch.setattr(
        remote_sessions,
        "list_archived_session_ids",
        lambda _config, _client: ["one", "two"],
    )

    def fake_pull(session_id, _paths, _config, force=False, client=None):
        barrier.wait(timeout=1)
        return _paths.archive / session_id

    monkeypatch.setattr(remote_sessions, "pull_session", fake_pull)

    summary = remote_sessions.pull_all_sessions(paths, config)

    assert summary.pulled == ["one", "two"]
    assert summary.skipped == []
    assert summary.failed == []


def test_ensure_local_session_does_not_contact_archive_when_local(tmp_path: Path) -> None:
    paths = _paths(tmp_path / "home")
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(paths)
    local = store.create(
        DirectorySession(
            "session",
            str(root.resolve()),
            "now",
            "now",
            SessionOrigin("1.0.0", "user", "host"),
            "complete",
        )
    )

    assert (
        ensure_local_session("session", paths, S3Config("bucket", "prefix"), client=object())
        == local
    )


def test_ensure_local_session_pulls_when_missing(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source_store, session = _published(_paths(tmp_path / "source-home"), root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(source_store, session, config, client)
    destination_paths = _paths(tmp_path / "destination-home")

    destination = ensure_local_session("session", destination_paths, config, client=client)

    assert destination == destination_paths.archive / "session"
    assert SessionStore(destination_paths).load_session("session").session_id == "session"


def test_remote_agent_inspection_streams_metadata_without_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    store, session = _published(
        _paths(tmp_path / "source-home"),
        root,
        content=os.urandom(2 * 1024 * 1024),
    )
    session_path = store.session_path("session")
    trace = session_path / "agents/traces/run.jsonl"
    metadata_path = session_path / "agents/runs/run.json"
    metadata = json.loads(metadata_path.read_text())
    metadata.update(
        {
            "agent_session_id": "native-session",
            "trace_complete_size": trace.stat().st_size,
            "trace_digest": hashlib.sha256(trace.read_bytes()).hexdigest(),
        }
    )
    atomic_write(metadata_path, (json.dumps(metadata) + "\n").encode())
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)

    runs, session_ids = inspect_archived_agent_runs(session.origin, config, client)

    assert session_ids == {"session"}
    assert runs[0]["native_id"] == "native-session"
    assert runs[0]["state"] == "complete"
    generation, body = next(item for item in client.response_bodies if item[0].endswith(".tar.zst"))
    assert body.was_closed
    assert body.bytes_read < len(client.objects[generation])


def _write_stream(session_path: Path) -> None:
    terminal = session_path / "streams/terminals/terminal"
    chunk = terminal / "chunks/events.jsonl.gz"
    chunk.parent.mkdir(parents=True)
    events = [
        StreamEvent("terminal", 1, "input", base64.b64encode(b"first\n").decode(), 1),
        StreamEvent("terminal", 2, "input", base64.b64encode(b"second\n").decode(), 2),
    ]
    atomic_write(
        chunk,
        gzip.compress(
            b"".join(json.dumps(event.to_dict()).encode() + b"\n" for event in events), mtime=0
        ),
    )
    atomic_write(
        terminal / "stream.json",
        (
            json.dumps(
                {
                    "schema_version": 1,
                    "terminal_id": "terminal",
                    "highest_sequence": 2,
                    "chunks": ["chunks/events.jsonl.gz"],
                }
            )
            + "\n"
        ).encode(),
    )


def _published(
    paths: StoragePaths, root: Path, content: bytes | None = None
) -> tuple[SessionStore, DirectorySession]:
    store = SessionStore(paths)
    session = DirectorySession(
        "session",
        str(root.resolve()),
        "now",
        "now",
        SessionOrigin("1.0.0", "user", "host"),
        state="complete",
    )
    directory = store.create(session)
    _write_stream(directory)
    trace = directory / "agents/traces/run.jsonl"
    trace.write_text('{"session_id":"native-session","type":"user","content":"trace prompt"}\n')
    AgentRunMetadata(
        run_id="run",
        harness="claude",
        model=None,
        reasoning=None,
        command=None,
        cwd=str(root.resolve()),
        started_utc="now",
        ended_utc="now",
        exit_code=None,
        agent_session_id="native-session",
        trace_file=trace.name,
        trace_complete_size=trace.stat().st_size,
        trace_digest=hashlib.sha256(trace.read_bytes()).hexdigest(),
    ).write(directory / "agents/runs/run.json")
    for step, high_water in ((0, 1), (1, 2)):
        prepared = Path(tempfile.mkdtemp(prefix="prepared-", dir=directory))
        data = content if content is not None and step == 1 else f"step {step}\n".encode()
        (prepared / "file.txt").write_bytes(data)
        manifest = StepManifest(
            session.session_id,
            step,
            "now",
            f"snapshots/{step}",
            [SnapshotEntry("file.txt", "file", 0o644, len(data))],
            {"terminal": high_water},
            agent_runs=[] if step == 0 else ["run"],
        )
        store.publish(session, manifest, prepared)
    return store, session


def _tar_zst(members: list[tuple[tarfile.TarInfo, bytes | None]]) -> bytes:
    raw = io.BytesIO()
    with zstandard.ZstdCompressor(level=3).stream_writer(raw, closefd=False) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as archive:
            for info, data in members:
                archive.addfile(info, io.BytesIO(data) if data is not None else None)
    return raw.getvalue()


def _replace_remote_package(client: FakeS3, package: bytes) -> dict[str, object]:
    completion_key = next(key for key in client.objects if "/completions/" in key)
    pointer = json.loads(client.objects.pop(completion_key))
    old_generation = str(pointer["generation"])
    client.objects.pop(old_generation)
    digest = hashlib.sha256(package).hexdigest()
    pointer["sha256"] = digest
    generation = str(
        Path(old_generation).with_name(f"{pointer['final_step']:08d}-{digest}.tar.zst")
    )
    pointer["generation"] = generation
    completion_key = str(
        Path(completion_key).with_name(f"{pointer['final_step']:08d}-{digest}.json")
    )
    client.objects[completion_key] = json.dumps(
        pointer, sort_keys=True, separators=(",", ":")
    ).encode()
    client.objects[generation] = package
    return pointer


def _archive_names(data: bytes) -> set[str]:
    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as reader:
        with tarfile.open(fileobj=reader, mode="r|") as archive:
            return {member.name for member in archive}


def test_package_is_deterministic_and_contains_complete_history(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    launches = store.session_path(session.session_id) / "agents" / "launches"
    launches.mkdir()
    (launches / "launch.json").write_text('{"kind":"sandbox-shell"}\n')
    first = prepare_generation(store, session)
    second = prepare_generation(store, session)
    try:
        first_data = first.path.read_bytes()
        second_data = second.path.read_bytes()
    finally:
        first.cleanup()
        second.cleanup()
    assert first_data == second_data
    assert first.digest == second.digest
    assert first.step == 1
    names = _archive_names(first_data)
    assert {
        "steps/0.json",
        "steps/1.json",
        "snapshots/0/file.txt",
        "snapshots/1/file.txt",
        "streams/terminals/terminal/stream.json",
        "streams/terminals/terminal/chunks/events.jsonl.gz",
        "agents/runs/run.json",
        "agents/traces/run.jsonl",
        "agents/launches/launch.json",
    }.issubset(names)


def test_git_snapshot_generation_extracts_and_validates_as_a_session(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    (root / "note.txt").write_text("captured")
    source_paths = _paths(tmp_path / "source-home")
    source_store = SessionStore(source_paths)
    session = DirectorySession(
        "git-session", str(root.resolve()), "now", "now", SessionOrigin("1.0.0", "user", "host")
    )
    source_store.create(session)
    manifest = StepPublisher(source_store).publish(session)
    prepared = prepare_generation(source_store, session)
    repeated = prepare_generation(source_store, session)
    try:
        assert prepared.path.read_bytes() == repeated.path.read_bytes()
        names = _archive_names(prepared.path.read_bytes())
        assert "snapshots.bundle" in names
        assert not any(
            name == "snapshots.git" or name.startswith("snapshots.git/") for name in names
        )
        extracted = tmp_path / "extracted"
        extracted.mkdir()
        with prepared.path.open("rb") as archive:
            safe_extract_tar_zst_stream(archive, extracted)
        remote_sessions._restore_snapshot_bundle(extracted, session.session_id)

        pulled_paths = _paths(tmp_path / "pulled-home")
        pulled_paths.archive.mkdir(parents=True)
        destination = pulled_paths.archive / session.session_id
        atomic_install_directory(extracted, destination)
        pulled_store = SessionStore(pulled_paths)
        pulled_manifest = pulled_store.steps(session.session_id)[-1]
        restored = tmp_path / "restored"
        pulled_store.restore_manifest(session.session_id, pulled_manifest, restored)
    finally:
        prepared.cleanup()
        repeated.cleanup()

    assert manifest.snapshot_commit == pulled_manifest.snapshot_commit
    assert (destination / "snapshots.git" / "HEAD").is_file()
    assert not (destination / "snapshots.bundle").exists()
    assert (restored / "note.txt").read_text() == "captured"


def test_git_session_round_trips_through_remote_transport(tmp_path: Path) -> None:
    source_paths = _paths(tmp_path / "source-home")
    source_store, session = _git_session(source_paths, tmp_path / "source")
    client = FakeS3()
    config = S3Config("bucket", "prefix")

    pushed = push_session(source_store, session, config, client)
    assert pushed["status"] == "pushed"

    pulled_paths = _paths(tmp_path / "pulled-home")
    pulled_path = pull_session(session.session_id, pulled_paths, config, client=client)
    pulled_store = SessionStore(pulled_paths)
    manifest = pulled_store.steps(session.session_id)[-1]
    restored = tmp_path / "restored"
    pulled_store.restore_manifest(session.session_id, manifest, restored)

    assert pulled_path == pulled_paths.archive / session.session_id
    assert manifest.snapshot_commit
    assert (restored / "note.txt").read_text() == "captured"
    # Steps keep their entry list in a shared pool, so the archive has to carry
    # it or a pulled recording cannot say what it captured. A plainly captured
    # file is not listed: the Git tree already records it.
    assert manifest.entries_digest
    assert manifest.entries == []
    assert (pulled_path / "entries" / f"{manifest.entries_digest}.json").is_file()


def test_corrupt_git_snapshot_archive_does_not_replace_existing_session(tmp_path: Path) -> None:
    source_paths = _paths(tmp_path / "source-home")
    source_store, session = _git_session(source_paths, tmp_path / "source")
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(source_store, session, config, client)

    completion_key = next(key for key in client.objects if "/completions/" in key)
    completion = json.loads(client.objects[completion_key])
    generation = completion["generation"]
    original = client.objects[generation]
    manifest = source_store.steps(session.session_id)[-1]
    assert manifest.snapshot_commit
    uncompressed = zstandard.ZstdDecompressor().decompress(
        original, max_output_size=64 * 1024 * 1024
    )
    raw = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(uncompressed), mode="r:") as source:
        with zstandard.ZstdCompressor(level=3).stream_writer(raw, closefd=False) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as target:
                for member in source.getmembers():
                    extracted = source.extractfile(member) if member.isfile() else None
                    data = extracted.read() if extracted is not None else None
                    if member.name == "snapshots.bundle":
                        assert data is not None
                        data = data[:-1] + bytes([data[-1] ^ 0xFF])
                    target.addfile(member, io.BytesIO(data) if data is not None else None)
    _replace_remote_package(client, raw.getvalue())

    existing_paths = _paths(tmp_path / "existing-home")
    existing = existing_paths.archive / session.session_id
    existing.mkdir(parents=True)
    (existing / "sentinel.txt").write_text("preserve")

    with pytest.raises(GitSnapshotError, match="git snapshot operation failed"):
        pull_session(session.session_id, existing_paths, config, force=True, client=client)

    assert (existing / "sentinel.txt").read_text() == "preserve"


def test_push_rejects_large_generation_before_upload(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    monkeypatch.setenv("MEMO_LARGE_ARCHIVE_BYTES", "0")

    with pytest.raises(ValueError, match="exceeding the configured limit"):
        push_session(store, session, S3Config("bucket", "prefix"), client)

    assert not [operation for operation in client.operations if operation[0] == "upload"]
    assert store.load_session(session.session_id).last_pushed_step is None


def test_push_publishes_immutable_generation_index_and_completion_and_skips_unchanged(
    tmp_path: Path,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    progress: list[tuple[int, int, str]] = []
    result = push_session(
        store,
        session,
        config,
        client,
        progress=lambda completed, total, message: progress.append((completed, total, message)),
    )
    assert result["status"] == "pushed"
    assert any(message == "creating archive" for _, _, message in progress)
    upload_progress = [event for event in progress if event[2] == "uploading archive"]
    assert upload_progress
    assert upload_progress[-1][0] == upload_progress[-1][1] > 0
    digest = str(result["digest"])
    generation = f"prefix/user/host/sessions/session/generations/00000001-{digest}.tar.zst"
    completion_key = f"prefix/user/host/sessions/session/completions/00000001-{digest}.json"
    index_key = next(
        key for key in client.objects if key.startswith("prefix/index/sessions/session/")
    )
    assert client.operations[-1] == ("put", completion_key)
    completion = json.loads(client.objects[completion_key])
    package = client.objects[generation]
    assert hashlib.sha256(package).hexdigest() == completion["sha256"]
    assert completion == {
        "schema_version": 1,
        "session_id": "session",
        "final_step": 1,
        "generation": generation,
        "sha256": result["digest"],
    }
    assert (
        client.operations.index(("upload", generation))
        < client.operations.index(("put", index_key))
        < client.operations.index(("put", completion_key))
    )
    assert not any(operation in {"copy", "delete"} for operation, _ in client.operations)
    index_data = client.objects[index_key]
    assert Path(index_key).stem == hashlib.sha256(index_data).hexdigest()
    index = json.loads(index_data)
    assert index == {
        "schema_version": 1,
        "session_id": "session",
        "memo_version_id": "1.0.0",
        "username": "user",
        "hostname": "host",
    }

    session.last_pushed_step = None
    session.last_pushed_digest = None
    session.remote_object = None
    store.update_session(session)
    client_two = FakeS3()
    second_result = push_session(store, session, config, client_two)
    assert client_two.objects[generation] == package
    assert second_result["digest"] == result["digest"]

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(package)) as reader:
        with tarfile.open(fileobj=reader, mode="r|") as archive:
            members = list(archive)
    assert [member.name for member in members] == sorted(member.name for member in members)
    assert all(member.uid == member.gid == member.mtime == 0 for member in members)
    assert all(member.uname == member.gname == "" for member in members)
    refreshed = store.load_session("session")
    before = list(client.operations)
    assert push_session(store, refreshed, config, client)["status"] == "skipped"
    assert client.operations[len(before) :] == [
        ("list", "prefix/user/host/sessions/session/completions/")
    ]


def test_push_delegates_large_file_upload_to_client(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(
        _paths(tmp_path / "home"), root, content=os.urandom(MULTIPART_PART_SIZE + 1024)
    )
    client = FakeS3()
    push_session(store, session, S3Config("bucket", "prefix"), client)
    assert len(client.part_sizes) >= 2
    assert client.part_sizes[0] == MULTIPART_PART_SIZE
    assert 0 < client.part_sizes[-1] < MULTIPART_PART_SIZE
    assert client.upload_options["part_size"] == MULTIPART_PART_SIZE
    assert client.upload_options["num_parallel_uploads"] == 3


def test_upload_failure_does_not_publish_or_advance_local_state(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    client.fail_operation = ("upload", None)

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, S3Config("bucket", "prefix"), client)

    assert not client.objects
    assert store.load_session("session").last_pushed_step is None


def test_failed_completion_marker_does_not_advance_local_state(
    tmp_path: Path,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    original_put = client.put_object

    def fail_completion(
        bucket: str, key: str, data, length: int, content_type: str | None = None
    ) -> None:
        if "/completions/" in key:
            raise OSError("injected completion failure")
        original_put(bucket, key, data, length, content_type)

    client.put_object = fail_completion  # type: ignore[method-assign]

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, config, client)
    assert not any("/completions/" in key for key in client.objects)
    assert any(key.endswith(".tar.zst") for key in client.objects)
    assert any("/index/sessions/session/" in key for key in client.objects)
    refreshed = store.load_session("session")
    assert refreshed.last_pushed_step is None


def test_index_failure_leaves_generation_but_not_local_state(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    original_put = client.put_object

    def fail_index(
        bucket: str, key: str, data, length: int, content_type: str | None = None
    ) -> None:
        if "/index/sessions/session/" in f"/{key}":
            raise OSError("injected index failure")
        original_put(bucket, key, data, length, content_type)

    client.put_object = fail_index  # type: ignore[method-assign]

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, S3Config("bucket", "prefix"), client)

    assert any(key.endswith(".tar.zst") for key in client.objects)
    assert not any("/index/sessions/session/" in key for key in client.objects)
    assert store.load_session("session").last_pushed_step is None


def test_retry_verifies_existing_objects_and_finishes_publication(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    original_put = client.put_object

    def fail_completion(
        bucket: str, key: str, data, length: int, content_type: str | None = None
    ) -> None:
        if "/completions/" in key:
            raise OSError("injected completion failure")
        original_put(bucket, key, data, length, content_type)

    client.put_object = fail_completion  # type: ignore[method-assign]
    with pytest.raises(OSError, match="injected"):
        push_session(store, session, config, client)

    client.put_object = original_put  # type: ignore[method-assign]
    result = push_session(store, store.load_session("session"), config, client)

    assert result["status"] == "pushed"
    assert any("/completions/" in key for key in client.objects)
    assert any(
        operation == "stat" and key.endswith(".tar.zst") for operation, key in client.operations
    )
    assert store.load_session("session").last_pushed_step == 1


def test_push_rejects_a_different_digest_for_the_same_step(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    conflicting = f"prefix/user/host/sessions/session/generations/00000001-{'0' * 64}.tar.zst"
    client.objects[conflicting] = b"other"

    with pytest.raises(ValueError, match="generation fork"):
        push_session(store, session, S3Config("bucket", "prefix"), client)

    assert store.load_session("session").last_pushed_step is None


def test_push_rejects_conflicting_content_addressed_index(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    index = f"prefix/index/sessions/session/{'0' * 64}.json"
    client.objects[index] = b'{"session_id":"someone-else"}'

    with pytest.raises(ValueError, match="index conflict"):
        push_session(store, session, S3Config("bucket", "prefix"), client)

    assert client.objects[index] == b'{"session_id":"someone-else"}'


def test_push_rejects_conflicting_completion(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    conflict_key = f"prefix/user/host/sessions/session/completions/00000000-{'0' * 64}.json"
    client.objects[conflict_key] = b"{}"

    with pytest.raises(ValueError, match="conflicting completion"):
        push_session(store, session, S3Config("bucket", "prefix"), client)

    assert client.objects[conflict_key] == b"{}"
    assert store.load_session("session").last_pushed_step is None


def test_pull_preserves_historical_replay_and_manifest_bounded_prompts(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)

    clean_paths = _paths(tmp_path / "clean-home")
    installed = pull_session("session", clean_paths, config, client=client)
    pulled = SessionStore(clean_paths)
    assert [manifest.step for manifest in pulled.steps("session")] == [0, 1]
    early = replay_session(
        "session", 0, tmp_path / "early", include_prompts=True, paths=clean_paths
    )
    latest = replay_session(
        "session", -1, tmp_path / "latest", include_prompts=True, paths=clean_paths
    )
    assert (early / "file.txt").read_text() == "step 0\n"
    assert "first" in (early / ".prompts.md").read_text()
    assert "second" not in (early / ".prompts.md").read_text()
    assert (latest / "file.txt").read_text() == "step 1\n"
    assert "second" in (latest / ".prompts.md").read_text()
    pulled_root = clean_paths.archive / "session"
    assert json.loads((pulled_root / "agents/runs/run.json").read_text())["harness"] == "claude"
    assert "trace prompt" in (pulled_root / "agents/traces/run.jsonl").read_text()
    assert installed == clean_paths.archive / "session"
    with pytest.raises(FileExistsError, match="not older"):
        pull_session("session", clean_paths, config, client=client)


def test_active_session_pull_uses_highest_complete_generation(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    session.state = "active"
    session.capture_scope = "agent-only"
    store.update_session(session)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)

    assert not any("/completions/" in key for key in client.objects)
    pulled_path = pull_session("session", _paths(tmp_path / "pulled-home"), config, client=client)
    assert SessionStore._validate_history(pulled_path, "session")[-1].step == 1


def test_completed_already_uploaded_generation_gets_completion_marker(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    session.state = "active"
    session.capture_scope = "agent-only"
    store.update_session(session)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    pushed = push_session(store, session, config, client)
    assert pushed["status"] == "pushed"
    assert not any("/completions/" in key for key in client.objects)

    completed = store.load_session("session")
    completed.state = "complete"
    store.update_session(completed)
    before_uploads = [operation for operation in client.operations if operation[0] == "upload"]

    republished = push_session(store, completed, config, client)

    assert republished["status"] == "pushed"
    assert [
        operation for operation in client.operations if operation[0] == "upload"
    ] == before_uploads
    assert any("/completions/" in key for key in client.objects)


def test_completion_marker_pins_pull_to_final_generation(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)
    base = "prefix/user/host/sessions/session/generations/"
    generation = next(key for key in client.objects if key.startswith(base))
    digest = Path(generation).name.split("-", 1)[1].removesuffix(".tar.zst")
    client.objects[f"{base}00000099-{digest}.tar.zst"] = client.objects[generation]

    pulled_path = pull_session("session", _paths(tmp_path / "pulled-home"), config, client=client)

    assert SessionStore._validate_history(pulled_path, "session")[-1].step == 1
    assert DirectorySession.load(pulled_path / "session.json").state == "complete"


def test_pull_verifies_checksum_and_remote_history_before_install(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)
    completion_key = next(key for key in client.objects if "/completions/" in key)
    completion = json.loads(client.objects[completion_key])
    generation = completion["generation"]

    original = client.objects[generation]
    client.objects[generation] = original + b"corrupt"
    corrupt_paths = _paths(tmp_path / "corrupt-home")
    with pytest.raises(ValueError, match="checksum mismatch"):
        pull_session("session", corrupt_paths, config, client=client)
    assert not corrupt_paths.archive.joinpath("session").exists()

    client.objects[generation] = original
    uncompressed = zstandard.ZstdDecompressor().decompress(
        original, max_output_size=64 * 1024 * 1024
    )
    raw = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(uncompressed), mode="r:") as source:
        with zstandard.ZstdCompressor(level=3).stream_writer(raw, closefd=False) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as target:
                for member in source.getmembers():
                    if member.name == "steps/0.json":
                        continue
                    target.addfile(member, source.extractfile(member) if member.isfile() else None)
    broken = raw.getvalue()
    assert hashlib.sha256(broken).hexdigest()
    _replace_remote_package(client, broken)
    incomplete_paths = _paths(tmp_path / "incomplete-home")
    with pytest.raises(ValueError, match="not contiguous"):
        pull_session("session", incomplete_paths, config, client=client)
    assert not incomplete_paths.archive.joinpath("session").exists()


def test_atomic_install_failure_restores_existing_session(tmp_path: Path, monkeypatch) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)
    paths = _paths(tmp_path / "home")
    destination = paths.archive / "session"
    destination.mkdir(parents=True)
    (destination / "local.txt").write_text("keep")

    from memo.transport import archive

    original_replace = os.replace
    calls = 0

    def fail_install(source: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("injected install failure")
        original_replace(source, target)

    monkeypatch.setattr(archive.os, "replace", fail_install)
    with pytest.raises(OSError, match="injected install failure"):
        pull_session("session", paths, config, force=True, client=client)
    assert (destination / "local.txt").read_text() == "keep"


def test_pull_streams_bounded_reads_and_closes_all_response_bodies(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)

    pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)

    assert len(client.response_bodies) == 3
    assert all(body.was_closed for _, body in client.response_bodies)
    assert all(
        body.read_sizes and max(body.read_sizes) <= 64 * 1024 for _, body in client.response_bodies
    )
    index_key = next(key for key in client.objects if "/index/sessions/session/" in key)
    completion_key = next(key for key in client.objects if "/completions/" in key)
    completion = json.loads(client.objects[completion_key])
    generation = completion["generation"]
    assert [key for operation, key in client.operations if operation == "get"] == [
        index_key,
        completion_key,
        generation,
    ]


def test_pull_rejects_index_with_invalid_content_digest(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)
    index_key = next(key for key in client.objects if "/index/sessions/session/" in key)
    client.objects[index_key] = b"{}"

    with pytest.raises(ValueError, match="index checksum"):
        pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)

    assert len(client.response_bodies) == 1
    assert all(body.was_closed for _, body in client.response_bodies)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", 2, "index"),
        ("session_id", "other", "index"),
        ("username", "", "invalid origin"),
    ],
)
def test_pull_rejects_invalid_index_before_package_request(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)
    index_key = next(key for key in client.objects if "/index/sessions/session/" in key)
    index = json.loads(client.objects.pop(index_key))
    index[field] = value
    index_data = json.dumps(index, sort_keys=True, separators=(",", ":")).encode()
    index_key = str(Path(index_key).with_name(f"{hashlib.sha256(index_data).hexdigest()}.json"))
    client.objects[index_key] = index_data

    with pytest.raises(ValueError, match=message):
        pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)

    assert len(client.response_bodies) == 1
    assert all(body.was_closed for _, body in client.response_bodies)


def test_pull_malformed_package_closes_body_and_removes_staging(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)
    malformed = b"not a zstandard stream"
    _replace_remote_package(client, malformed)
    destination_paths = _paths(tmp_path / "clean-home")

    with pytest.raises(zstandard.ZstdError):
        pull_session("session", destination_paths, config, client=client)

    assert client.response_bodies[-1][1].was_closed
    archive = destination_paths.archive
    assert not (archive / "session").exists()
    assert not list(archive.glob(".session.pull-*"))


def _regular(name: str, data: bytes = b"data") -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    return info, data


def _directory(name: str) -> tuple[tarfile.TarInfo, None]:
    info = tarfile.TarInfo(name)
    info.type = tarfile.DIRTYPE
    return info, None


@pytest.mark.parametrize(
    ("members", "message"),
    [
        ([_regular("../escape")], "unsafe archive path"),
        ([_regular("/absolute")], "unsafe archive path"),
        ([(tarfile.TarInfo("link"), None)], "unsupported archive entry"),
        ([(tarfile.TarInfo("hard"), None)], "unsupported archive entry"),
        ([(tarfile.TarInfo("device"), None)], "unsupported archive entry"),
        ([_regular("same"), _regular("same")], "duplicate archive entry"),
        ([_regular("parent"), _regular("parent/child")], "archive path conflict"),
        ([_regular("parent/child"), _regular("parent")], "archive path conflict"),
        ([_regular("valid-prefix"), _regular("../late-escape")], "unsafe archive path"),
    ],
    ids=[
        "traversal",
        "absolute",
        "symlink",
        "hardlink",
        "device",
        "duplicate",
        "file-parent",
        "file-after-child",
        "late-unsafe",
    ],
)
def test_pull_rejects_malicious_members_and_removes_staging(
    tmp_path: Path,
    members: list[tuple[tarfile.TarInfo, bytes | None]],
    message: str,
) -> None:
    if message == "unsupported archive entry":
        info = members[0][0]
        if info.name == "link":
            info.type = tarfile.SYMTYPE
            info.linkname = "target"
        elif info.name == "hard":
            info.type = tarfile.LNKTYPE
            info.linkname = "target"
        else:
            info.type = tarfile.CHRTYPE
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(store, session, config, client)
    _replace_remote_package(client, _tar_zst(members))
    destination_paths = _paths(tmp_path / "clean-home")

    with pytest.raises(ValueError, match=message):
        pull_session("session", destination_paths, config, client=client)

    archive = destination_paths.archive
    assert not (archive / "session").exists()
    assert not list(archive.glob(".session.pull-*"))
    assert client.response_bodies[-1][1].was_closed


def test_large_package_has_bounded_parts_reads_and_memory(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    content = os.urandom(MULTIPART_PART_SIZE * 2 + 1024 * 1024)
    store, session = _published(_paths(tmp_path / "source-home"), root, content=content)
    client = FakeS3()
    config = S3Config("bucket", "prefix")

    tracemalloc.start()
    push_session(store, session, config, client)
    pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(client.part_sizes) >= 3
    assert max(client.part_sizes) <= MULTIPART_PART_SIZE
    assert all(size <= 64 * 1024 for _, body in client.response_bodies for size in body.read_sizes)
    assert peak < 5 * MULTIPART_PART_SIZE


def test_origin_values_are_encoded_and_preserved_across_pull_and_repush(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source_paths = _paths(tmp_path / "source-home")
    store, session = _published(source_paths, root)
    session.origin = SessionOrigin("1.0.0", "user/name", "host name")
    store.update_session(session)
    config = S3Config("bucket", "prefix")
    client = FakeS3()

    push_session(store, session, config, client)

    base = "prefix/user%2Fname/host%20name/sessions/session"
    assert any(key.startswith(f"{base}/completions/") for key in client.objects)
    index_key = next(key for key in client.objects if "/index/sessions/session/" in key)
    index = json.loads(client.objects[index_key])
    assert index["username"] == "user/name"
    assert index["hostname"] == "host name"
    pulled_paths = _paths(tmp_path / "pulled-home")
    pulled_path = pull_session("session", pulled_paths, config, client=client)
    pulled = DirectorySession.load(pulled_path / "session.json")
    assert pulled.origin == session.origin

    pulled.last_pushed_step = None
    pulled.last_pushed_digest = None
    pulled.remote_object = None
    pulled_store = SessionStore(pulled_paths)
    pulled_store.update_session(pulled)
    second = FakeS3()
    push_session(pulled_store, pulled, config, second)
    assert any(key.startswith(f"{base}/completions/") for key in second.objects)


def _strip_from_remote_package(client: FakeS3, removed: str) -> None:
    """Rewrite the published generation without ``removed`` or anything beneath it.

    A directory named as the target keeps its own entry, so the archive still
    claims to carry snapshot content while holding none -- which is the state a
    real archive was found in.
    """
    completion_key = next(key for key in client.objects if "/completions/" in key)
    generation = json.loads(client.objects[completion_key])["generation"]
    uncompressed = zstandard.ZstdDecompressor().decompress(
        client.objects[generation], max_output_size=64 * 1024 * 1024
    )
    raw = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(uncompressed), mode="r:") as source:
        with zstandard.ZstdCompressor(level=3).stream_writer(raw, closefd=False) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as target:
                for member in source.getmembers():
                    if member.name == removed and not member.isdir():
                        continue
                    if member.name.startswith(removed) and member.name != removed:
                        continue
                    target.addfile(member, source.extractfile(member) if member.isfile() else None)
    _replace_remote_package(client, raw.getvalue())


def test_verify_accepts_a_restorable_generation(tmp_path: Path) -> None:
    source_paths = _paths(tmp_path / "source-home")
    source_store, session = _git_session(source_paths, tmp_path / "source")
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(source_store, session, config, client)

    result = remote_sessions.verify_archived_session(
        session.session_id, _paths(tmp_path / "verify-home"), config, client=client
    )

    assert result["session_id"] == session.session_id
    assert result["steps"] >= 1


def test_verify_reports_a_generation_with_no_snapshot_content(tmp_path: Path) -> None:
    source_paths = _paths(tmp_path / "source-home")
    source_store, session = _git_session(source_paths, tmp_path / "source")
    client = FakeS3()
    config = S3Config("bucket", "prefix")
    push_session(source_store, session, config, client)

    # An archive whose steps reference snapshot commits but whose object store
    # never made it up. The bytes are intact and the checksum matches, so the
    # push bookkeeping shows nothing wrong; only reading it back finds this.
    _strip_from_remote_package(client, "snapshots.bundle")

    with pytest.raises(ValueError, match="missing snapshot commit"):
        remote_sessions.verify_archived_session(
            session.session_id, _paths(tmp_path / "verify-home"), config, client=client
        )
