from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from memo.recording.metadata import DirectorySession, SessionOrigin, SnapshotEntry, StepManifest
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore
from memo.transport import remote_sessions
from memo.transport.archive import PreparedGeneration
from memo.transport.config import S3Config
from memo.transport.s3 import S3Store
from memo_legacy_migrator.s3_recompress import (
    _RemoteSource,
    _Replacement,
    _convert_snapshots,
    _install_replacement,
)


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.operations: list[tuple[str, str]] = []
        self.corrupt_uploads = False

    def fput_object(self, bucket: str, key: str, file_path: str, **kwargs) -> None:
        del bucket, kwargs
        self.operations.append(("upload", key))
        data = Path(file_path).read_bytes()
        self.objects[key] = data + b"corrupt" if self.corrupt_uploads else data

    def put_object(self, bucket: str, key: str, data, length: int, **kwargs) -> None:
        del bucket, kwargs
        self.operations.append(("put", key))
        self.objects[key] = data.read(length)

    def get_object(self, bucket: str, key: str) -> io.BytesIO:
        del bucket
        self.operations.append(("get", key))
        return io.BytesIO(self.objects[key])

    def stat_object(self, bucket: str, key: str) -> object:
        del bucket
        self.operations.append(("stat", key))
        if key not in self.objects:
            raise KeyError(key)
        return SimpleNamespace(size=len(self.objects[key]))

    def list_objects(self, bucket: str, prefix: str, recursive: bool = True):
        del bucket, recursive
        self.operations.append(("list", prefix))
        return (
            SimpleNamespace(object_name=key)
            for key in sorted(self.objects)
            if key.startswith(prefix)
        )

    def remove_object(self, bucket: str, key: str) -> None:
        del bucket
        self.operations.append(("remove", key))
        self.objects.pop(key, None)


def _replacement(tmp_path: Path, client: FakeS3) -> tuple[S3Store, _Replacement, str]:
    config = S3Config("bucket", "prefix")
    remote = S3Store(config, client)
    session = DirectorySession(
        "session",
        "/recorded/root",
        "now",
        "now",
        SessionOrigin("1.0.0", "user", "host"),
        state="complete",
    )
    base = remote_sessions._session_base(config, session.origin, session.session_id)
    old_data = b"original archive"
    old_digest = hashlib.sha256(old_data).hexdigest()
    old_generation = remote_sessions._generation_key(base, 0, old_digest)
    old_completion = remote_sessions._completion_key(base, 0, old_digest)
    old_completion_data = remote_sessions._canonical_json(
        {
            "schema_version": 1,
            "session_id": session.session_id,
            "final_step": 0,
            "generation": old_generation,
            "sha256": old_digest,
        }
    )
    client.objects[old_generation] = old_data
    client.objects[old_completion] = old_completion_data
    index_key, index_data = remote_sessions._index_record(config, session)
    client.objects[index_key] = index_data

    candidate = tmp_path / "candidate.tar.zst"
    candidate.write_bytes(b"verified replacement")
    candidate_digest = hashlib.sha256(candidate.read_bytes()).hexdigest()
    prepared = PreparedGeneration(
        session.session_id,
        1,
        candidate_digest,
        candidate,
        candidate.stat().st_size,
    )
    source = _RemoteSource(
        session.session_id,
        0,
        old_generation,
        old_digest,
        old_completion,
        old_completion_data,
    )
    return remote, _Replacement(source, session, prepared, len(old_data)), old_generation


def test_convert_snapshots_preserves_every_filesystem_step(tmp_path: Path) -> None:
    paths = StoragePaths(tmp_path / "home")
    store = SessionStore(paths)
    session = DirectorySession(
        "session",
        "/recorded/root",
        "now",
        "now",
        SessionOrigin("1.0.0", "user", "host"),
    )
    session_root = store.create(session)
    expected = []
    for step, content in enumerate((b"first", b"second")):
        snapshot = session_root / f"prepared-{step}"
        snapshot.mkdir()
        (snapshot / "file.bin").write_bytes(content)
        manifest = StepManifest(
            session.session_id,
            step,
            "now",
            f"snapshots/{step}",
            [SnapshotEntry("file.bin", "file", 0o644, len(content))],
        )
        store.publish(session, manifest, snapshot)
        expected.append(content)

    tree_ids = _convert_snapshots(session_root, session.session_id)
    manifests = store.steps(session.session_id)

    assert len(tree_ids) == 3
    assert tree_ids[-1] == tree_ids[-2]
    assert all(manifest.snapshot_commit for manifest in manifests)
    for index, content in enumerate((*expected, expected[-1])):
        restored = tmp_path / f"restored-{index}"
        store.restore_manifest(session.session_id, manifests[index], restored)
        assert (restored / "file.bin").read_bytes() == content


def test_install_replacement_verifies_before_removing_original(tmp_path: Path) -> None:
    client = FakeS3()
    remote, replacement, old_generation = _replacement(tmp_path, client)

    _install_replacement(remote, remote.config, replacement)

    assert old_generation not in client.objects
    remove_original = client.operations.index(("remove", old_generation))
    candidate_gets = [
        index
        for index, operation in enumerate(client.operations)
        if operation[0] == "get" and "migration-staging" not in operation[1]
    ]
    assert candidate_gets
    assert remove_original > max(candidate_gets)
    selected = remote_sessions._select_generation(
        remote,
        remote.config,
        old_generation.rsplit("/generations/", 1)[0],
        "session",
    )
    assert selected[0] == 1
    assert selected[3] is True


def test_install_replacement_preserves_original_when_staging_is_corrupt(
    tmp_path: Path,
) -> None:
    client = FakeS3()
    remote, replacement, old_generation = _replacement(tmp_path, client)
    original = client.objects[old_generation]
    client.corrupt_uploads = True

    with pytest.raises(ValueError, match="staged replacement"):
        _install_replacement(remote, remote.config, replacement)

    assert client.objects[old_generation] == original
    assert replacement.source.completion_key in client.objects
    assert ("remove", old_generation) not in client.operations
