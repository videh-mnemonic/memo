from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
import tempfile
from pathlib import Path

import pytest
import zstandard

from memo.config import Paths, TransportConfig
from memo.models import CheckpointManifest, DirectorySession, SnapshotEntry
from memo.session_store import SessionStore
from memo.transport import (MULTIPART_PART_SIZE, MultipartUploadWriter,
                            pull_session, push_session, safe_extract_bytes)


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

    @staticmethod
    def _bytes(value) -> bytes:
        return value.read() if hasattr(value, "read") else bytes(value)

    def put_object(self, *, Bucket: str, Key: str, Body) -> None:
        self.operations.append(("put", Key))
        if Key == self.fail_key:
            raise OSError("injected upload failure")
        self.objects[Key] = self._bytes(Body)

    def create_multipart_upload(self, *, Bucket: str, Key: str) -> dict[str, str]:
        self.operations.append(("create_multipart", Key))
        upload_id = f"upload-{len(self.uploads) + 1}"
        self.uploads[upload_id] = {"key": Key, "parts": {}}
        return {"UploadId": upload_id}

    def upload_part(self, *, Bucket: str, Key: str, UploadId: str,
                    PartNumber: int, Body) -> dict[str, str]:
        self.operations.append(("upload_part", f"{Key}:{PartNumber}"))
        if self.fail_operation == ("upload_part", PartNumber):
            raise OSError("injected part upload failure")
        data = self._bytes(Body)
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
        self.aborted.add(UploadId)

    def copy_object(self, *, Bucket: str, Key: str, CopySource: dict[str, str]) -> None:
        self.operations.append(("copy", Key))
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.operations.append(("delete", Key))
        self.objects.pop(Key, None)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, TrackingBody]:
        self.operations.append(("get", Key))
        body = TrackingBody(self.objects[Key])
        self.response_bodies.append((Key, body))
        return {"Body": body}

    def list_objects_v2(self, *, Bucket: str, Prefix: str) -> dict[str, object]:
        return {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(Prefix)]}


def _paths(root: Path) -> Paths:
    return Paths(root, root / "scratch", root / "archive", root / "unpack")


def _published(paths: Paths, root: Path, generation: int = 1,
               content: bytes | None = None) -> tuple[SessionStore, DirectorySession]:
    store = SessionStore(paths)
    session = DirectorySession("session", str(root.resolve()), "namespace", "now", "now",
                               state="complete")
    directory = store.create(session)
    checkpoint_id = f"checkpoint-{generation}"
    prepared = Path(tempfile.mkdtemp(prefix="prepared-", dir=directory))
    data = content if content is not None else f"generation {generation}\n".encode()
    (prepared / "file.txt").write_bytes(data)
    manifest = CheckpointManifest(
        checkpoint_id, session.session_id, generation, "now",
        f"snapshots/{checkpoint_id}",
        [SnapshotEntry("file.txt", "file", 0o644, len(data))],
    )
    store.publish(session, manifest, prepared)
    return store, session


def test_push_package_is_deterministic_and_unchanged_generation_is_skipped(tmp_path: Path) -> None:
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

    session.last_pushed_generation = None
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
    assert not any("/generations/" in key for key in client.objects)
    assert store.load_session("namespace", "session").last_pushed_generation is None


def test_failed_final_publication_does_not_advance_local_or_remote_pointer(tmp_path: Path) -> None:
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
    assert refreshed.last_pushed_generation is None


def test_pull_verifies_checksum_and_refuses_local_conflict(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_paths = _paths(tmp_path / "source-home")
    store, session = _published(source_paths, source_root)
    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    push_session(store, session, config, client)

    clean_paths = _paths(tmp_path / "clean-home")
    installed = pull_session("session", clean_paths, config, client=client)
    assert (installed / "snapshots" / "checkpoint-1" / "file.txt").read_text() == "generation 1\n"
    with pytest.raises(FileExistsError, match="not older"):
        pull_session("session", clean_paths, config, client=client)

    pointer = json.loads(client.objects["prefix/namespace/session/latest.json"])
    client.objects[pointer["object"]] += b"corrupt"
    other_paths = _paths(tmp_path / "other-home")
    with pytest.raises(ValueError, match="checksum mismatch"):
        pull_session("session", other_paths, config, client=client)
    assert not other_paths.archive.joinpath("namespace", "session").exists()


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
        ("schema_version", 2, "schema"),
        ("session_id", "other", "session identity"),
        ("namespace", "other", "object identity"),
        ("object", "prefix/namespace/session/generations/package.tar.gz", ".tar.zst"),
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


def test_safe_extract_rejects_traversal(tmp_path: Path) -> None:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:gz") as archive:
        info = tarfile.TarInfo("../escape")
        info.size = 3
        archive.addfile(info, io.BytesIO(b"bad"))
    with pytest.raises(ValueError, match="unsafe archive path"):
        safe_extract_bytes(raw.getvalue(), tmp_path / "target")
