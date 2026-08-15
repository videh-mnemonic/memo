from __future__ import annotations

import base64
import gzip
import io
import json
import os
import tarfile
import tempfile
from pathlib import Path

import pytest

from memo.config import Paths, TransportConfig
from memo.load import replay_session
from memo.models import DirectorySession, SnapshotEntry, StepManifest
from memo.session_store import SessionStore, atomic_write
from memo.streams import StreamEvent
from memo.transport import (package_history, pull_session, push_session,
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


def _published(paths: Paths, root: Path) -> tuple[SessionStore, DirectorySession]:
    store = SessionStore(paths)
    session = DirectorySession(
        "session", str(root.resolve()), "namespace", "now", "now", state="complete"
    )
    directory = store.create(session)
    _write_stream(directory)
    for step, high_water in ((0, 1), (1, 2)):
        prepared = Path(tempfile.mkdtemp(prefix="prepared-", dir=directory))
        content = f"step {step}\n"
        (prepared / "file.txt").write_text(content)
        manifest = StepManifest(
            session.session_id,
            step,
            "now",
            f"snapshots/{step}",
            [SnapshotEntry("file.txt", "file", 0o644, len(content))],
            {"terminal": high_water},
        )
        store.publish(session, manifest, prepared)
    return store, session


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
    pointer = json.loads(client.objects[latest])
    assert pointer["schema_version"] == 2
    assert pointer["step"] == 1
    assert "/steps/1-" in pointer["object"]
    assert client.operations.index(("copy", pointer["object"])) < client.operations.index(
        ("put", pointer["checksum"])
    ) < client.operations.index(("put", latest))
    assert client.operations[-1][0] == "delete"

    refreshed = store.load_session("namespace", "session")
    before = list(client.operations)
    assert push_session(store, refreshed, config, client)["status"] == "skipped"
    assert client.operations == before


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
    raw = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(original), mode="r:gz") as source:
        with tarfile.open(fileobj=raw, mode="w:gz") as target:
            for member in source.getmembers():
                if member.name == "steps/0.json":
                    continue
                target.addfile(member, source.extractfile(member) if member.isfile() else None)
    from memo.transport import digest_bytes
    broken = raw.getvalue()
    digest = digest_bytes(broken)
    client.objects[pointer["object"]] = broken
    client.objects[pointer["checksum"]] = f"{digest}  package.tar.gz\n".encode()
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


def test_safe_extract_rejects_traversal(tmp_path: Path) -> None:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:gz") as archive:
        info = tarfile.TarInfo("../escape")
        info.size = 3
        archive.addfile(info, io.BytesIO(b"bad"))
    with pytest.raises(ValueError, match="unsafe archive path"):
        safe_extract_bytes(raw.getvalue(), tmp_path / "target")
