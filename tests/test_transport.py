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

from memo.config import Paths, TransportConfig
from memo.load import replay_session
from memo.models import DirectorySession, SnapshotEntry, StepManifest
from memo.session_store import SessionStore, atomic_write
from memo.streams import StreamEvent
from memo.transport import (MULTIPART_PART_SIZE, MultipartUploadWriter,
                            package_history, pull_session, push_session,
                            safe_extract_bytes)


class TrackingBody(io.BytesIO):
    def __init__(self, data: bytes, max_chunk: int = 4096) -> None:
        super().__init__(data)
        self.max_chunk = max_chunk
        self.read_sizes: list[int] = []
        self.was_closed = False

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            raise AssertionError("unbounded response body read")
        self.read_sizes.append(size)
        return super().read(min(size, self.max_chunk))

    def close(self) -> None:
        self.was_closed = True
        super().close()


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

    def put_object(self, *, Bucket: str, Key: str, Body) -> None:
        self.operations.append(("put", Key))
        if (Key == self.fail_key or self.fail_operation == ("put", Key)
                or (self.fail_operation == ("put_checksum", None) and Key.endswith(".sha256"))):
            raise OSError("injected upload failure")
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
                                  MultipartUpload: dict[str, object]) -> None:
        self.operations.append(("complete_multipart", Key))
        if self.fail_operation == ("complete_multipart", None):
            raise OSError("injected multipart completion failure")
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

    def copy_object(self, *, Bucket: str, Key: str, CopySource: dict[str, str]) -> None:
        self.operations.append(("copy", Key))
        if self.fail_operation == ("copy", None):
            raise OSError("injected copy failure")
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.operations.append(("delete", Key))
        if self.fail_operation == ("delete", None):
            raise OSError("injected delete failure")
        self.objects.pop(Key, None)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, TrackingBody]:
        self.operations.append(("get", Key))
        body = TrackingBody(self.objects[Key])
        self.response_bodies.append((Key, body))
        return {"Body": body}

    def list_objects_v2(self, *, Bucket: str, Prefix: str) -> dict[str, object]:
        return {"Contents": [
            {"Key": key} for key in sorted(self.objects) if key.startswith(Prefix)
        ]}


def _paths(root: Path) -> Paths:
    return Paths(root)


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


def _published(paths: Paths, root: Path,
               content: bytes | None = None) -> tuple[SessionStore, DirectorySession]:
    store = SessionStore(paths)
    session = DirectorySession(
        "session", str(root.resolve()), "namespace", "now", "now", state="complete"
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
    latest = "prefix/namespace/session/latest.json"
    pointer = json.loads(client.objects[latest])
    digest = hashlib.sha256(package).hexdigest()
    pointer["digest"] = digest
    client.objects[latest] = json.dumps(pointer).encode()
    client.objects[str(pointer["checksum"])] = f"{digest}  package.tar.zst\n".encode()
    client.objects[str(pointer["object"])] = package
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


def test_push_publishes_step_object_checksum_then_pointer_and_skips_unchanged(
    tmp_path: Path,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    result = push_session(store, session, config, client)
    assert result["status"] == "pushed"
    latest = "prefix/namespace/session/latest.json"
    assert client.operations[-1] == ("put", latest)
    pointer = json.loads(client.objects[latest])
    assert pointer["object"].endswith(".tar.zst")
    package = client.objects[pointer["object"]]
    assert hashlib.sha256(package).hexdigest() == pointer["digest"]
    assert client.operations.index(("put", pointer["checksum"])) < client.operations.index(("put", latest))
    temporary = next(key for operation, key in client.operations if operation == "create_multipart")
    assert client.operations.index(("complete_multipart", temporary)) < client.operations.index(
        ("copy", pointer["object"])
    )
    assert client.operations.index(("put", pointer["checksum"])) < client.operations.index(
        ("delete", temporary)
    )
    assert client.operations.index(("delete", temporary)) < client.operations.index(("put", latest))

    session.last_pushed_step = None
    session.last_pushed_digest = None
    session.remote_object = None
    store.update_session(session)
    client_two = FakeS3()
    second_result = push_session(store, session, config, client_two)
    second_pointer = json.loads(client_two.objects[latest])
    assert client_two.objects[second_pointer["object"]] == package
    assert second_result["digest"] == result["digest"]

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(package)) as reader:
        with tarfile.open(fileobj=reader, mode="r|") as archive:
            members = list(archive)
    assert [member.name for member in members] == sorted(member.name for member in members)
    assert all(member.uid == member.gid == member.mtime == 0 for member in members)
    assert all(member.uname == member.gname == "" for member in members)
    pointer = json.loads(client.objects[latest])
    assert pointer["schema_version"] == 2
    assert pointer["step"] == 1
    assert "/steps/1-" in pointer["object"]
    assert client.operations.index(("copy", pointer["object"])) < client.operations.index(
        ("put", pointer["checksum"])
    ) < client.operations.index(("put", latest))
    assert pointer["object"].endswith(".tar.zst")

    refreshed = store.load_session("namespace", "session")
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
    latest = "prefix/namespace/session/latest.json"
    client.objects[latest] = b'{"old": true}'
    client.fail_operation = failure

    with pytest.raises(OSError, match=message):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert client.aborted == {"upload-1"}
    assert client.objects[latest] == b'{"old": true}'
    assert not any("/steps/" in key for key in client.objects)
    assert store.load_session("namespace", "session").last_pushed_step is None


def test_failed_final_publication_does_not_advance_local_or_remote_pointer(
    tmp_path: Path,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    latest = "prefix/namespace/session/latest.json"
    client.objects[latest] = b'{"old": true}'
    client.fail_key = latest

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, config, client)
    assert client.objects[latest] == b'{"old": true}'
    refreshed = store.load_session("namespace", "session")
    assert refreshed.last_pushed_step is None


@pytest.mark.parametrize(
    ("failure", "abort_expected", "temporary_expected", "step_expected"),
    [
        (("create_multipart", None), False, False, False),
        (("upload_part", 1), True, False, False),
        (("complete_multipart", None), True, False, False),
        (("copy", None), False, False, False),
        (("put_checksum", None), False, False, True),
        (("delete", None), False, True, True),
    ],
)
def test_push_failure_boundaries_preserve_old_pointer_and_local_state(
    tmp_path: Path, failure: tuple[str, object], abort_expected: bool,
    temporary_expected: bool, step_expected: bool,
) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    latest = "prefix/namespace/session/latest.json"
    client.objects[latest] = b'{"old": true}'
    client.fail_operation = failure

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert client.objects[latest] == b'{"old": true}'
    assert store.load_session("namespace", "session").last_pushed_step is None
    assert bool(client.aborted) is abort_expected
    temporary_keys = [key for key in client.objects if "/tmp/" in key]
    assert bool(temporary_keys) is temporary_expected
    step_keys = [key for key in client.objects
                 if "/steps/" in key and not key.endswith(".sha256")]
    assert bool(step_keys) is step_expected


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


def test_pointer_failure_leaves_completed_generation_but_not_local_state(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    client = FakeS3()
    latest = "prefix/namespace/session/latest.json"
    client.objects[latest] = b'{"old": true}'
    client.fail_key = latest

    with pytest.raises(OSError, match="injected"):
        push_session(store, session, TransportConfig("bucket", "prefix"), client)

    assert client.objects[latest] == b'{"old": true}'
    assert not any("/tmp/" in key for key in client.objects)
    assert any(key.endswith(".tar.zst") for key in client.objects)
    assert any(key.endswith(".sha256") for key in client.objects)
    assert store.load_session("namespace", "session").last_pushed_step is None


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
    assert [manifest.step for manifest in pulled.steps("namespace", "session")] == [0, 1]
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
    pulled_root = clean_paths.archive / "namespace/session"
    assert json.loads((pulled_root / "agents/runs/run.json").read_text())["harness"] == "claude"
    assert "trace prompt" in (pulled_root / "agents/traces/run.jsonl").read_text()
    assert installed == clean_paths.archive / "namespace/session"
    with pytest.raises(FileExistsError, match="not older"):
        pull_session("session", clean_paths, config, client=client)


def test_pull_verifies_checksum_and_remote_history_before_install(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    pointer = json.loads(client.objects["prefix/namespace/session/latest.json"])

    original = client.objects[pointer["object"]]
    client.objects[pointer["object"]] = original + b"corrupt"
    corrupt_paths = _paths(tmp_path / "corrupt-home")
    with pytest.raises(ValueError, match="checksum mismatch"):
        pull_session("session", corrupt_paths, config, client=client)
    assert not corrupt_paths.archive.joinpath("namespace", "session").exists()

    client.objects[pointer["object"]] = original
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
    client.objects[pointer["object"]] = broken
    client.objects[pointer["checksum"]] = f"{digest}  package.tar.zst\n".encode()
    pointer["digest"] = digest
    client.objects["prefix/namespace/session/latest.json"] = json.dumps(pointer).encode()
    incomplete_paths = _paths(tmp_path / "incomplete-home")
    with pytest.raises(ValueError, match="not contiguous"):
        pull_session("session", incomplete_paths, config, client=client)
    assert not incomplete_paths.archive.joinpath("namespace", "session").exists()


def test_atomic_install_failure_restores_existing_session(tmp_path: Path, monkeypatch) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    paths = _paths(tmp_path / "home")
    destination = paths.archive / "namespace/session"
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

    assert len(client.response_bodies) == 3
    assert all(body.was_closed for _, body in client.response_bodies)
    assert all(body.read_sizes and max(body.read_sizes) <= 64 * 1024
               for _, body in client.response_bodies)
    latest = "prefix/namespace/session/latest.json"
    pointer = json.loads(client.objects[latest])
    assert [key for operation, key in client.operations if operation == "get"] == [
        latest, pointer["checksum"], pointer["object"]
    ]


def test_pull_closes_metadata_body_when_sidecar_disagrees(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    pointer = json.loads(client.objects["prefix/namespace/session/latest.json"])
    client.objects[pointer["checksum"]] = b"0" * 64 + b"  package.tar.zst\n"

    with pytest.raises(ValueError, match="pointer and checksum disagree"):
        pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)

    assert len(client.response_bodies) == 2
    assert all(body.was_closed for _, body in client.response_bodies)
    assert not any(key == pointer["object"] for operation, key in client.operations
                   if operation == "get")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", 1, "schema"),
        ("session_id", "other", "session identity"),
        ("namespace", "other", "object identity"),
        ("object", "prefix/namespace/session/steps/package.tar.gz", ".tar.zst"),
    ],
)
def test_pull_rejects_invalid_pointer_before_package_request(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    latest = "prefix/namespace/session/latest.json"
    pointer = json.loads(client.objects[latest])
    pointer[field] = value
    client.objects[latest] = json.dumps(pointer).encode()

    with pytest.raises(ValueError, match=message):
        pull_session("session", _paths(tmp_path / "clean-home"), config, client=client)

    assert len(client.response_bodies) == 1
    assert client.response_bodies[0][1].was_closed


def test_pull_malformed_package_closes_body_and_removes_staging(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    store, session = _published(_paths(tmp_path / "source-home"), source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)
    pointer = json.loads(client.objects["prefix/namespace/session/latest.json"])
    malformed = b"not a zstandard stream"
    digest = hashlib.sha256(malformed).hexdigest()
    pointer["digest"] = digest
    client.objects["prefix/namespace/session/latest.json"] = json.dumps(pointer).encode()
    client.objects[pointer["checksum"]] = f"{digest}  package.tar.zst\n".encode()
    client.objects[pointer["object"]] = malformed
    destination_paths = _paths(tmp_path / "clean-home")

    with pytest.raises(zstandard.ZstdError):
        pull_session("session", destination_paths, config, client=client)

    assert client.response_bodies[-1][1].was_closed
    namespace = destination_paths.archive / "namespace"
    assert not (namespace / "session").exists()
    assert not list(namespace.glob(".session.pull-*"))


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

    namespace = destination_paths.archive / "namespace"
    assert not (namespace / "session").exists()
    assert not list(namespace.glob(".session.pull-*"))
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
