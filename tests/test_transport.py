from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import os
import tarfile
import tempfile
import tracemalloc
from pathlib import Path

import pytest
import zstandard

from memo.config import StoragePaths, TransportConfig
from memo.export import replay_session
from memo.recording.models import DirectorySession, SessionOrigin, SnapshotEntry, StepManifest
from memo.recording.store import SessionStore, atomic_write
from memo.recording.streams import StreamEvent
from memo.transport import (MULTIPART_PART_SIZE, MultipartUploadWriter,
                            ensure_local_session, inspect_archived_agent_runs,
                            list_archived_session_ids,
                            package_history, pull_session, push_session,
                            safe_extract_bytes)


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


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.operations: list[tuple[str, str]] = []
        self.fail_key: str | None = None
        self.fail_operation: tuple[str, object] | None = None
        self.uploads: dict[str, dict[str, object]] = {}
        self.aborted: set[str] = set()
        self.response_bodies: list[tuple[str, TrackingBody]] = []
        self.part_sizes: list[int] = []

    @staticmethod
    def _bytes(value) -> bytes:
        return value.read() if hasattr(value, "read") else bytes(value)

    def put_object(self, *, Bucket: str, Key: str, Body,
                   IfNoneMatch: str | None = None) -> None:
        self.operations.append(("put", Key))
        if (Key == self.fail_key or self.fail_operation == ("put", Key)
                or (self.fail_operation == ("put_checksum", None) and Key.endswith(".sha256"))):
            raise OSError("injected upload failure")
        if IfNoneMatch == "*" and Key in self.objects:
            raise FakeS3Error("PreconditionFailed")
        self.objects[Key] = self._bytes(Body)

    def create_multipart_upload(self, *, Bucket: str, Key: str) -> dict[str, str]:
        self.operations.append(("create_multipart", Key))
        if self.fail_operation == ("create_multipart", None):
            raise OSError("injected multipart initiation failure")
        upload_id = f"upload-{len(self.uploads) + 1}"
        self.uploads[upload_id] = {"key": Key, "parts": {}}
        return {"UploadId": upload_id}

    def upload_part(self, *, Bucket: str, Key: str, UploadId: str,
                    PartNumber: int, Body) -> dict[str, str]:
        self.operations.append(("upload_part", f"{Key}:{PartNumber}"))
        if self.fail_operation == ("upload_part", PartNumber):
            raise OSError("injected part upload failure")
        data = self._bytes(Body)
        self.part_sizes.append(len(data))
        parts = self.uploads[UploadId]["parts"]
        assert isinstance(parts, dict)
        parts[PartNumber] = data
        return {"ETag": hashlib.md5(data).hexdigest()}

    def complete_multipart_upload(self, *, Bucket: str, Key: str, UploadId: str,
                                  MultipartUpload: dict[str, object],
                                  IfNoneMatch: str | None = None) -> None:
        self.operations.append(("complete_multipart", Key))
        if self.fail_operation == ("complete_multipart", None):
            raise OSError("injected multipart completion failure")
        if IfNoneMatch == "*" and Key in self.objects:
            raise FakeS3Error("PreconditionFailed")
        requested = MultipartUpload["Parts"]
        assert isinstance(requested, list)
        numbers = [part["PartNumber"] for part in requested]
        assert numbers == sorted(numbers)
        parts = self.uploads[UploadId]["parts"]
        assert isinstance(parts, dict)
        self.objects[Key] = b"".join(parts[number] for number in numbers)

    def abort_multipart_upload(self, *, Bucket: str, Key: str, UploadId: str) -> None:
        self.operations.append(("abort_multipart", Key))
        if self.fail_operation == ("abort_multipart", None):
            raise OSError("injected multipart abort failure")
        self.aborted.add(UploadId)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, TrackingBody]:
        self.operations.append(("get", Key))
        body = TrackingBody(self.objects[Key])
        self.response_bodies.append((Key, body))
        return {"Body": body}

    def list_objects_v2(self, *, Bucket: str, Prefix: str,
                        ContinuationToken: str | None = None) -> dict[str, object]:
        self.operations.append(("list", Prefix))
        return {"Contents": [
            {"Key": key} for key in sorted(self.objects) if key.startswith(Prefix)
        ]}


def _paths(root: Path) -> StoragePaths:
    return StoragePaths(root)


def test_list_archived_session_ids_uses_index_and_filters_invalid_keys() -> None:
    client = FakeS3()
    client.objects.update({
        "prefix/index/sessions/b.json": b"{}",
        "prefix/index/sessions/a.json": b"{}",
        "prefix/index/sessions/nested/session.json": b"{}",
        "prefix/index/sessions/readme.txt": b"",
        "prefix/unrelated.json": b"{}",
    })

    assert list_archived_session_ids(
        TransportConfig("bucket", "prefix"), client
    ) == ["a", "b"]


def test_ensure_local_session_does_not_contact_archive_when_local(tmp_path: Path) -> None:
    paths = _paths(tmp_path / "home")
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(paths)
    local = store.create(DirectorySession(
        "session", str(root.resolve()), "now", "now",
        SessionOrigin("1.0.0", "user", "host"), "complete",
    ))

    assert ensure_local_session(
        "session", paths, TransportConfig("bucket", "prefix"), client=object()
    ) == local


def test_ensure_local_session_pulls_when_missing(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source_store, session = _published(_paths(tmp_path / "source-home"), root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(source_store, session, config, client)
    destination_paths = _paths(tmp_path / "destination-home")

    destination = ensure_local_session(
        "session", destination_paths, config, client=client
    )

    assert destination == destination_paths.archive / "session"
    assert SessionStore(destination_paths).load_session("session").session_id == "session"


def test_remote_agent_inspection_streams_metadata_without_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    store, session = _published(
        _paths(tmp_path / "source-home"), root, content=os.urandom(2 * 1024 * 1024),
    )
    session_path = store.session_path("session")
    trace = session_path / "agents/traces/run.jsonl"
    metadata_path = session_path / "agents/runs/run.json"
    metadata = json.loads(metadata_path.read_text())
    metadata.update({
        "agent_session_id": "native-session",
        "trace_complete_size": trace.stat().st_size,
        "trace_digest": hashlib.sha256(trace.read_bytes()).hexdigest(),
    })
    atomic_write(metadata_path, (json.dumps(metadata) + "\n").encode())
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)

    runs, session_ids = inspect_archived_agent_runs(session.origin, config, client)

    assert session_ids == {"session"}
    assert runs[0]["native_id"] == "native-session"
    generation, body = next(
        item for item in client.response_bodies if item[0].endswith(".tar.zst")
    )
    assert body.was_closed
    assert body.bytes_read < len(client.objects[generation])


def test_remote_agent_inspection_hashes_legacy_trace_without_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    store, session = _published(
        _paths(tmp_path / "source-home"), root, content=os.urandom(2 * 1024 * 1024),
    )
    metadata_path = store.session_path("session") / "agents/runs/run.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["agent_session_id"] = "legacy-native"
    atomic_write(metadata_path, (json.dumps(metadata) + "\n").encode())
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)

    runs, _ = inspect_archived_agent_runs(session.origin, config, client)

    assert runs[0]["native_id"] == "legacy-native"
    assert runs[0]["complete_size"] > 0
    generation, body = next(
        item for item in client.response_bodies if item[0].endswith(".tar.zst")
    )
    assert body.bytes_read < len(client.objects[generation])


def _write_stream(session_path: Path) -> None:
    terminal = session_path / "streams/terminals/terminal"
    chunk = terminal / "chunks/events.jsonl.gz"
    chunk.parent.mkdir(parents=True)
    events = [
        StreamEvent("terminal", 1, "input", base64.b64encode(b"first\n").decode(), 1),
        StreamEvent("terminal", 2, "input", base64.b64encode(b"second\n").decode(), 2),
    ]
    atomic_write(chunk, gzip.compress(b"".join(
        json.dumps(event.to_dict()).encode() + b"\n" for event in events
    ), mtime=0))
    atomic_write(terminal / "stream.json", (json.dumps({
        "schema_version": 1,
        "terminal_id": "terminal",
        "highest_sequence": 2,
        "chunks": ["chunks/events.jsonl.gz"],
    }) + "\n").encode())


def _published(paths: StoragePaths, root: Path,
               content: bytes | None = None) -> tuple[SessionStore, DirectorySession]:
    store = SessionStore(paths)
    session = DirectorySession(
        "session", str(root.resolve()), "now", "now",
        SessionOrigin("1.0.0", "user", "host"), state="complete"
    )
    directory = store.create(session)
    _write_stream(directory)
    (directory / "agents/traces/run.jsonl").write_text(
        '{"session_id":"native-session","type":"user","content":"trace prompt"}\n'
    )
    atomic_write(directory / "agents/runs/run.json", (json.dumps({
        "run_id": "run",
        "harness": "claude",
        "trace_file": "run.jsonl",
    }) + "\n").encode())
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
    completion_key = "prefix/user/host/sessions/session/completion.json"
    pointer = json.loads(client.objects[completion_key])
    digest = hashlib.sha256(package).hexdigest()
    pointer["sha256"] = digest
    client.objects[completion_key] = json.dumps(pointer).encode()
    generation = str(pointer["generation"])
    checksum = generation.removesuffix(".tar.zst") + ".sha256"
    client.objects[checksum] = f"{digest}  {Path(generation).name}\n".encode()
    client.objects[generation] = package
    return pointer


def _archive_names(data: bytes) -> set[str]:
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        return {member.name for member in archive.getmembers()}


def test_package_is_deterministic_and_contains_complete_history(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    first, first_digest, manifest = package_history(store, session)
    second, second_digest, _ = package_history(store, session)
    assert first == second
    assert first_digest == second_digest
    assert manifest.step == 1
    names = _archive_names(first)
    assert {
        "steps/0.json", "steps/1.json", "snapshots/0/file.txt",
        "snapshots/1/file.txt", "streams/terminals/terminal/stream.json",
        "streams/terminals/terminal/chunks/events.jsonl.gz",
        "agents/runs/run.json", "agents/traces/run.jsonl",
    }.issubset(names)


def test_push_publishes_immutable_generation_index_and_completion_and_skips_unchanged(
    tmp_path: Path,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    result = push_session(store, session, config, client)
    assert result["status"] == "pushed"
    generation = "prefix/user/host/sessions/session/generations/00000001.tar.zst"
    checksum = "prefix/user/host/sessions/session/generations/00000001.sha256"
    completion_key = "prefix/user/host/sessions/session/completion.json"
    assert client.operations[-2:] == [
        ("put", "prefix/index/sessions/session.json"), ("put", completion_key)
    ]
    completion = json.loads(client.objects[completion_key])
    package = client.objects[generation]
    assert hashlib.sha256(package).hexdigest() == completion["sha256"]
    assert completion == {
        "schema_version": 1, "session_id": "session", "final_step": 1,
        "generation": generation, "sha256": result["digest"],
    }
    assert client.operations.index(("complete_multipart", generation)) < \
        client.operations.index(("put", checksum)) < \
        client.operations.index(("put", completion_key))
    assert not any(operation in {"copy", "delete"} for operation, _ in client.operations)
    index = json.loads(client.objects["prefix/index/sessions/session.json"])
    assert index == {
        "schema_version": 1, "session_id": "session", "memo_version_id": "1.0.0",
        "username": "user", "hostname": "host",
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
    assert client.operations == before


def test_multipart_writer_uploads_full_parts_and_short_final_part() -> None:
    client = FakeS3()
    upload_id = client.create_multipart_upload(Bucket="bucket", Key="key")["UploadId"]
    writer = MultipartUploadWriter(client, "bucket", "key", upload_id, part_size=5)
    writer.write(b"abcdefghijkl")
    parts = writer.finish()
    assert [part["PartNumber"] for part in parts] == [1, 2, 3]
    uploaded = client.uploads[upload_id]["parts"]
    assert isinstance(uploaded, dict)
    assert [len(uploaded[number]) for number in sorted(uploaded)] == [5, 5, 2]


def test_push_uses_multiple_fixed_size_multipart_parts(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(
        _paths(tmp_path / "home"), root, content=os.urandom(MULTIPART_PART_SIZE + 1024)
    )
    client = FakeS3()
    push_session(store, session, TransportConfig("bucket", "prefix"), client)
    upload = next(iter(client.uploads.values()))
    parts = upload["parts"]
    assert isinstance(parts, dict)
    assert len(parts) >= 2
    assert len(parts[1]) == MULTIPART_PART_SIZE
    assert 0 < len(parts[max(parts)]) < MULTIPART_PART_SIZE


@pytest.mark.parametrize(
    ("failure", "message"),
    [(('upload_part', 1), "part upload"), (('complete_multipart', None), "completion")],
)
def test_multipart_failure_aborts_without_publication(
    tmp_path: Path, failure: tuple[str, object], message: str
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    client.fail_operation = failure

    with pytest.raises(OSError, match=message):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert client.aborted == {"upload-1"}
    assert not client.objects
    assert store.load_session("session").last_pushed_step is None


def test_failed_completion_marker_does_not_advance_local_state(
    tmp_path: Path,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    completion = "prefix/user/host/sessions/session/completion.json"
    client.fail_key = completion

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, config, client)
    assert completion not in client.objects
    assert any(key.endswith(".tar.zst") for key in client.objects)
    assert any(key.endswith(".sha256") for key in client.objects)
    refreshed = store.load_session("session")
    assert refreshed.last_pushed_step is None


@pytest.mark.parametrize(
    ("failure", "abort_expected", "generation_expected"),
    [
        (("create_multipart", None), False, False),
        (("upload_part", 1), True, False),
        (("complete_multipart", None), True, False),
        (("put_checksum", None), False, True),
    ],
)
def test_push_failure_boundaries_preserve_local_state(
    tmp_path: Path, failure: tuple[str, object], abort_expected: bool,
    generation_expected: bool,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    client.fail_operation = failure

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert store.load_session("session").last_pushed_step is None
    assert bool(client.aborted) is abort_expected
    generation_keys = [key for key in client.objects if key.endswith(".tar.zst")]
    assert bool(generation_keys) is generation_expected


def test_abort_failure_preserves_part_upload_failure(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    def fail_part_and_abort(**kwargs):
        client.operations.append(("upload_part", f"{kwargs['Key']}:{kwargs['PartNumber']}"))
        client.fail_operation = ("abort_multipart", None)
        raise OSError("injected part upload failure")

    client.upload_part = fail_part_and_abort  # type: ignore[method-assign]
    with pytest.raises(OSError, match="part upload"):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)
    assert any(operation == "abort_multipart" for operation, _ in client.operations)


def test_index_failure_leaves_generation_but_not_local_state(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    index = "prefix/index/sessions/session.json"
    client.fail_key = index

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert any(key.endswith(".tar.zst") for key in client.objects)
    assert any(key.endswith(".sha256") for key in client.objects)
    assert index not in client.objects
    assert store.load_session("session").last_pushed_step is None


def test_retry_verifies_existing_objects_and_finishes_publication(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    completion = "prefix/user/host/sessions/session/completion.json"
    client.fail_key = completion
    with pytest.raises(OSError, match="injected"):
        push_session(store, session, config, client)

    client.fail_key = None
    result = push_session(store, store.load_session("session"), config, client)

    assert result["status"] == "pushed"
    assert completion in client.objects
    assert any(operation == "get" and key.endswith(".tar.zst")
               for operation, key in client.operations)
    assert store.load_session("session").last_pushed_step == 1


def test_retry_recovers_generation_without_checksum(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    client.fail_operation = ("put_checksum", None)
    with pytest.raises(OSError, match="injected"):
        push_session(store, session, config, client)
    assert any(key.endswith(".tar.zst") for key in client.objects)
    assert not any(key.endswith(".sha256") for key in client.objects)

    client.fail_operation = None
    push_session(store, store.load_session("session"), config, client)

    assert any(key.endswith(".sha256") for key in client.objects)
    assert "prefix/user/host/sessions/session/completion.json" in client.objects


def test_retry_rejects_existing_generation_with_different_content(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    generation = "prefix/user/host/sessions/session/generations/00000001.tar.zst"
    client.objects[generation] = b"poisoned"

    with pytest.raises(ValueError, match="integrity conflict"):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert client.objects[generation] == b"poisoned"
    assert store.load_session("session").last_pushed_step is None


def test_retry_rejects_conflicting_write_once_index(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    index = "prefix/index/sessions/session.json"
    client.objects[index] = b'{"session_id":"someone-else"}'

    with pytest.raises(ValueError, match="integrity conflict"):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert client.objects[index] == b'{"session_id":"someone-else"}'


@pytest.mark.parametrize("conflict_key", [
    "prefix/user/host/sessions/session/generations/00000001.sha256",
    "prefix/user/host/sessions/session/completion.json",
])
def test_retry_rejects_conflicting_checksum_or_completion(
    tmp_path: Path, conflict_key: str,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    client.objects[conflict_key] = b"poisoned"

    with pytest.raises(ValueError, match="integrity conflict"):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert client.objects[conflict_key] == b"poisoned"
    assert store.load_session("session").last_pushed_step is None


def test_pull_preserves_historical_replay_and_manifest_bounded_prompts(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
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
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)

    assert "prefix/user/host/sessions/session/completion.json" not in client.objects
    pulled_path = pull_session(
        "session", _paths(tmp_path / "pulled-home"), config, client=client
    )
    assert SessionStore._validate_history(pulled_path, "session")[-1].step == 1


def test_completion_marker_pins_pull_to_final_generation(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    base = "prefix/user/host/sessions/session/generations/"
    client.objects[f"{base}00000099.tar.zst"] = client.objects[f"{base}00000001.tar.zst"]
    client.objects[f"{base}00000099.sha256"] = client.objects[f"{base}00000001.sha256"]

    pulled_path = pull_session(
        "session", _paths(tmp_path / "pulled-home"), config, client=client
    )

    assert SessionStore._validate_history(pulled_path, "session")[-1].step == 1


def test_pull_paginates_generation_listing(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    session.state = "active"
    store.update_session(session)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    original_list = client.list_objects_v2

    def paginated_list(*, Bucket: str, Prefix: str,
                       ContinuationToken: str | None = None) -> dict[str, object]:
        all_items = original_list(Bucket=Bucket, Prefix=Prefix)["Contents"]
        assert isinstance(all_items, list)
        if ContinuationToken is None:
            return {"Contents": all_items[:1], "IsTruncated": True,
                    "NextContinuationToken": "next"}
        assert ContinuationToken == "next"
        return {"Contents": all_items[1:], "IsTruncated": False}

    client.list_objects_v2 = paginated_list  # type: ignore[method-assign]
    pulled = pull_session("session", _paths(tmp_path / "pulled-home"), config, client=client)
    assert SessionStore._validate_history(pulled, "session")[-1].step == 1


def test_pull_verifies_checksum_and_remote_history_before_install(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    completion_key = "prefix/user/host/sessions/session/completion.json"
    completion = json.loads(client.objects[completion_key])
    generation = completion["generation"]
    checksum = generation.removesuffix(".tar.zst") + ".sha256"

    original = client.objects[generation]
    client.objects[generation] = original + b"corrupt"
    corrupt_paths = _paths(tmp_path / "corrupt-home")
    with pytest.raises(ValueError, match="checksum mismatch"):
        pull_session("session", corrupt_paths, config, client=client)
    assert not corrupt_paths.archive.joinpath("session").exists()

    client.objects[generation] = original
    uncompressed = zstandard.ZstdDecompressor().decompress(original, max_output_size=64 * 1024 * 1024)
    raw = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(uncompressed), mode="r:") as source:
        with zstandard.ZstdCompressor(level=3).stream_writer(raw, closefd=False) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as target:
                for member in source.getmembers():
                    if member.name == "steps/0.json":
                        continue
                    target.addfile(member, source.extractfile(member) if member.isfile() else None)
    from memo.transport import digest_bytes
    broken = raw.getvalue()
    digest = digest_bytes(broken)
    client.objects[generation] = broken
    client.objects[checksum] = f"{digest}  {Path(generation).name}\n".encode()
    completion["sha256"] = digest
    client.objects[completion_key] = json.dumps(completion).encode()
    incomplete_paths = _paths(tmp_path / "incomplete-home")
    with pytest.raises(ValueError, match="not contiguous"):
        pull_session("session", incomplete_paths, config, client=client)
    assert not incomplete_paths.archive.joinpath("session").exists()


def test_atomic_install_failure_restores_existing_session(tmp_path: Path, monkeypatch) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    paths = _paths(tmp_path / "home")
    destination = paths.archive / "session"
    destination.mkdir(parents=True)
    (destination / "local.txt").write_text("keep")

    from memo import transport
    original_replace = os.replace
    calls = 0
    def fail_install(source: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("injected install failure")
        original_replace(source, target)
    monkeypatch.setattr(transport.os, "replace", fail_install)
    with pytest.raises(OSError, match="injected install failure"):
        pull_session("session", paths, config, force=True, client=client)
    assert (destination / "local.txt").read_text() == "keep"


def test_pull_streams_bounded_reads_and_closes_all_response_bodies(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)

    pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)

    assert len(client.response_bodies) == 4
    assert all(body.was_closed for _, body in client.response_bodies)
    assert all(body.read_sizes and max(body.read_sizes) <= 64 * 1024
               for _, body in client.response_bodies)
    completion_key = "prefix/user/host/sessions/session/completion.json"
    completion = json.loads(client.objects[completion_key])
    generation = completion["generation"]
    checksum = generation.removesuffix(".tar.zst") + ".sha256"
    assert [key for operation, key in client.operations if operation == "get"] == [
        "prefix/index/sessions/session.json", completion_key,
        checksum, generation,
    ]


def test_pull_closes_metadata_body_when_sidecar_disagrees(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    completion = json.loads(
        client.objects["prefix/user/host/sessions/session/completion.json"]
    )
    generation = completion["generation"]
    checksum = generation.removesuffix(".tar.zst") + ".sha256"
    client.objects[checksum] = b"0" * 64 + b"  package.tar.zst\n"

    with pytest.raises(ValueError, match="completion marker and checksum disagree"):
        pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)

    assert len(client.response_bodies) == 3
    assert all(body.was_closed for _, body in client.response_bodies)
    assert not any(key == generation for operation, key in client.operations
                   if operation == "get")


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
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    index_key = "prefix/index/sessions/session.json"
    index = json.loads(client.objects[index_key])
    index[field] = value
    client.objects[index_key] = json.dumps(index).encode()

    with pytest.raises(ValueError, match=message):
        pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)

    assert len(client.response_bodies) == 1
    assert all(body.was_closed for _, body in client.response_bodies)


def test_pull_malformed_package_closes_body_and_removes_staging(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    completion_key = "prefix/user/host/sessions/session/completion.json"
    completion = json.loads(client.objects[completion_key])
    generation = completion["generation"]
    checksum = generation.removesuffix(".tar.zst") + ".sha256"
    malformed = b"not a zstandard stream"
    digest = hashlib.sha256(malformed).hexdigest()
    completion["sha256"] = digest
    client.objects[completion_key] = json.dumps(completion).encode()
    client.objects[checksum] = f"{digest}  {Path(generation).name}\n".encode()
    client.objects[generation] = malformed
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
    ids=["traversal", "absolute", "symlink", "hardlink", "device", "duplicate",
         "file-parent", "file-after-child", "late-unsafe"],
)
def test_pull_rejects_malicious_members_and_removes_staging(
    tmp_path: Path, members: list[tuple[tarfile.TarInfo, bytes | None]], message: str,
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
    config = TransportConfig("bucket", "prefix")
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
    config = TransportConfig("bucket", "prefix")

    tracemalloc.start()
    push_session(store, session, config, client)
    pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(client.part_sizes) >= 3
    assert max(client.part_sizes) <= MULTIPART_PART_SIZE
    assert all(size <= 64 * 1024 for _, body in client.response_bodies
               for size in body.read_sizes)
    assert peak < 5 * MULTIPART_PART_SIZE


def test_safe_extract_rejects_traversal(tmp_path: Path) -> None:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:gz") as archive:
        info = tarfile.TarInfo("../escape")
        info.size = 3
        archive.addfile(info, io.BytesIO(b"bad"))
    with pytest.raises(ValueError, match="unsafe archive path"):
        safe_extract_bytes(raw.getvalue(), tmp_path / "target")


def test_origin_values_are_encoded_and_preserved_across_pull_and_repush(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source_paths = _paths(tmp_path / "source-home")
    store, session = _published(source_paths, root)
    session.origin = SessionOrigin("1.0.0", "user/name", "host name")
    store.update_session(session)
    config = TransportConfig("bucket", "prefix")
    client = FakeS3()

    push_session(store, session, config, client)

    base = "prefix/user%2Fname/host%20name/sessions/session"
    assert f"{base}/completion.json" in client.objects
    index = json.loads(client.objects["prefix/index/sessions/session.json"])
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
    assert f"{base}/completion.json" in second.objects
