from __future__ import annotations

import io
import json
import tarfile
import tempfile
from pathlib import Path

import pytest

from memo.config import Paths, TransportConfig
from memo.models import CheckpointManifest, DirectorySession, SnapshotEntry
from memo.session_store import SessionStore
from memo.transport import (package_generation, pull_session, push_session,
                            safe_extract_bytes)


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.operations: list[tuple[str, str]] = []
        self.fail_key: str | None = None

    @staticmethod
    def _bytes(value) -> bytes:
        return value.read() if hasattr(value, "read") else bytes(value)

    def put_object(self, *, Bucket: str, Key: str, Body) -> None:
        self.operations.append(("put", Key))
        if Key == self.fail_key:
            raise OSError("injected upload failure")
        self.objects[Key] = self._bytes(Body)

    def copy_object(self, *, Bucket: str, Key: str, CopySource: dict[str, str]) -> None:
        self.operations.append(("copy", Key))
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.operations.append(("delete", Key))
        self.objects.pop(Key, None)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, io.BytesIO]:
        return {"Body": io.BytesIO(self.objects[Key])}

    def list_objects_v2(self, *, Bucket: str, Prefix: str) -> dict[str, object]:
        return {"Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(Prefix)]}


def _paths(root: Path) -> Paths:
    return Paths(root)


def _published(paths: Paths, root: Path, generation: int = 1) -> tuple[SessionStore, DirectorySession]:
    store = SessionStore(paths)
    session = DirectorySession("session", str(root.resolve()), "namespace", "now", "now",
                               state="complete")
    directory = store.create(session)
    checkpoint_id = f"checkpoint-{generation}"
    prepared = Path(tempfile.mkdtemp(prefix="prepared-", dir=directory))
    (prepared / "file.txt").write_text(f"generation {generation}\n")
    manifest = CheckpointManifest(
        checkpoint_id, session.session_id, generation, "now",
        f"snapshots/{checkpoint_id}",
        [SnapshotEntry("file.txt", "file", 0o644, len(f"generation {generation}\n"))],
    )
    store.publish(session, manifest, prepared)
    return store, session


def test_package_is_deterministic_and_unchanged_generation_is_skipped(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    store, session = _published(_paths(tmp_path / "home"), root)
    first, first_digest, _ = package_generation(store, session)
    second, second_digest, _ = package_generation(store, session)
    assert first == second
    assert first_digest == second_digest

    client = FakeS3()
    config = TransportConfig("bucket", "prefix")
    result = push_session(store, session, config, client)
    assert result["status"] == "pushed"
    latest = "prefix/namespace/session/latest.json"
    assert client.operations[-2] == ("put", latest)
    assert client.operations[-1][0] == "delete"
    pointer = json.loads(client.objects[latest])
    assert client.operations.index(("put", pointer["checksum"])) < client.operations.index(("put", latest))

    refreshed = store.load_session("namespace", "session")
    before = list(client.operations)
    assert push_session(store, refreshed, config, client)["status"] == "skipped"
    assert client.operations == before


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


def test_safe_extract_rejects_traversal(tmp_path: Path) -> None:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:gz") as archive:
        info = tarfile.TarInfo("../escape")
        info.size = 3
        archive.addfile(info, io.BytesIO(b"bad"))
    with pytest.raises(ValueError, match="unsafe archive path"):
        safe_extract_bytes(raw.getvalue(), tmp_path / "target")
