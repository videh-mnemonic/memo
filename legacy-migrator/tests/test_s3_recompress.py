from __future__ import annotations

import gzip
import hashlib
import io
import json
import shutil
import tarfile
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import memo_legacy_migrator.s3_recompress as s3_recompress
import pytest
from memo_legacy_migrator.s3_recompress import (
    RemoteCandidate,
    _install_replacement,
    _prepare_replacement,
    discover_remote_candidates,
    recompress_s3,
    source_for_candidate,
)
from memo_legacy_migrator.session_upgrade import AlreadyLatest, upgrade_session

from memo.recording.git_snapshots import GitSnapshotStore
from memo.recording.metadata import (
    STEP_SCHEMA_VERSION,
    DirectorySession,
    SessionOrigin,
    SnapshotEntry,
    StepManifest,
    digest_entries,
    encode_entries,
)
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore
from memo.transport import remote_sessions
from memo.transport.archive import prepare_generation, safe_extract_tar_zst_stream
from memo.transport.config import S3Config
from memo.transport.s3 import S3Store

ORIGIN = SessionOrigin("0.9.0", "user", "host")


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.operations: list[tuple[str, str]] = []
        self.corrupt_uploads = False
        self.corrupt_final_uploads = False

    def fput_object(self, bucket: str, key: str, file_path: str, **kwargs) -> None:
        del bucket, kwargs
        self.operations.append(("upload", key))
        data = Path(file_path).read_bytes()
        corrupt = self.corrupt_uploads or (
            self.corrupt_final_uploads and "/migration-staging/" not in key
        )
        self.objects[key] = data + b"corrupt" if corrupt else data

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


def _json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _session_value(session_id: str, version: int, *, origin: bool = True) -> dict[str, object]:
    value: dict[str, object] = {
        "format": "memo-directory-session",
        "format_version": version,
        "session_id": session_id,
        "root": "/recorded/root",
        "created_utc": "2025-01-01T00:00:00Z",
        "updated_utc": "2025-01-01T00:00:01Z",
        "state": "complete",
        "capture_scope": "partial",
    }
    if origin:
        value["origin"] = asdict(ORIGIN)
    else:
        value["archive_namespace"] = "legacy-host"
    return value


def _common_directories(root: Path) -> None:
    (root / "streams" / "terminals").mkdir(parents=True)
    (root / "agents" / "runs").mkdir(parents=True)
    (root / "agents" / "traces").mkdir(parents=True)


def _set_session_state(root: Path, state: str) -> None:
    value = json.loads((root / "session.json").read_text())
    value["state"] = state
    _json(root / "session.json", value)


def _checkpoint_session(root: Path, session_id: str) -> list[bytes]:
    root.mkdir(parents=True)
    _common_directories(root)
    _json(root / "session.json", _session_value(session_id, 1, origin=False))
    expected = [b"checkpoint one", b"checkpoint two"]
    for generation, content in enumerate(expected, 1):
        checkpoint_id = f"checkpoint-{generation}"
        snapshot = root / "snapshots" / checkpoint_id
        snapshot.mkdir(parents=True)
        (snapshot / "file.bin").write_bytes(content)
        _json(
            root / "checkpoints" / f"{checkpoint_id}.json",
            {
                "session_id": session_id,
                "checkpoint_id": checkpoint_id,
                "schema_version": 1,
                "generation": generation,
                "created_utc": f"time-{generation}",
                "snapshot": f"snapshots/{checkpoint_id}",
                "entries": [asdict(SnapshotEntry("file.bin", "file", 0o644, len(content)))],
            },
        )
    (root / "HEAD").write_text("checkpoint-2\n")
    return expected


def _numeric_session(
    root: Path,
    session_id: str,
    schemas: list[int],
    *,
    complete_fields: bool = False,
) -> list[bytes]:
    root.mkdir(parents=True)
    _common_directories(root)
    session_value = _session_value(session_id, 2)
    if complete_fields:
        session_value.update(
            last_pushed_step=None,
            last_pushed_digest=None,
            remote_object=None,
        )
    _json(root / "session.json", session_value)
    expected = [f"step {step}".encode() for step in range(len(schemas))]
    repository = GitSnapshotStore(root / "snapshots.git")
    parent = None
    for step, (schema, content) in enumerate(zip(schemas, expected, strict=True)):
        snapshot = root / "snapshots" / str(step)
        snapshot.mkdir(parents=True)
        (snapshot / "file.bin").write_bytes(content)
        entries = [
            SnapshotEntry("file.bin", "file", 0o644, len(content)),
            SnapshotEntry("ignored", "ignored-policy", 0),
        ]
        commit = None
        entries_digest = None
        stored_entries = entries
        if schema >= 2:
            commit = repository.commit(snapshot, parent, f"old step {step}")
            parent = commit
            shutil.rmtree(snapshot)
        if schema == STEP_SCHEMA_VERSION:
            stored_entries = [entries[-1]]
            entries_digest = digest_entries(stored_entries)
            (root / "entries").mkdir(exist_ok=True)
            (root / "entries" / f"{entries_digest}.json").write_bytes(
                encode_entries(stored_entries)
            )
        manifest = StepManifest(
            session_id,
            step,
            f"time-{step}",
            f"snapshots/{step}",
            stored_entries,
            schema_version=schema,
            snapshot_commit=commit,
            entries_digest=entries_digest,
        )
        _json(root / "steps" / f"{step}.json", manifest.to_stored_dict())
    (root / "HEAD").write_text(f"{len(schemas) - 1}\n")
    return expected


def _assert_latest(root: Path, session_id: str, expected: list[bytes], *, boundary: bool) -> None:
    session = DirectorySession.load(root / "session.json")
    assert session.origin == ORIGIN
    store = SessionStore(
        StoragePaths(
            root.parent,
            archive=root.parent,
            runtime=root.parent / "runtime-test",
            spool=root.parent / "spool-test",
        )
    )
    manifests = store.steps(session_id)
    assert len(manifests) == len(expected) + int(boundary)
    assert all(manifest.schema_version == STEP_SCHEMA_VERSION for manifest in manifests)
    assert all(manifest.snapshot_commit and manifest.entries_digest for manifest in manifests)
    for step, content in enumerate(expected + ([expected[-1]] if boundary else [])):
        restored = root.parent / f"restore-{session_id}-{step}"
        store.restore_manifest(session_id, manifests[step], restored)
        assert (restored / "file.bin").read_bytes() == content


@pytest.mark.parametrize(
    ("builder", "schemas", "source_format"),
    [
        ("checkpoint", [], "directory-v1-checkpoints"),
        ("numeric", [1, 1], "directory-v2-steps-1-directories"),
        ("numeric", [1, 2], "directory-v2-steps-1-2-repository"),
        ("numeric", [2, 2], "directory-v2-steps-2-repository"),
        ("numeric", [3, 3], "directory-v2-steps-3-repository"),
    ],
)
def test_upgrade_session_handles_every_directory_representation(
    tmp_path: Path, builder: str, schemas: list[int], source_format: str
) -> None:
    root = tmp_path / "session"
    expected = (
        _checkpoint_session(root, "session")
        if builder == "checkpoint"
        else _numeric_session(root, "session", schemas)
    )

    result = upgrade_session(
        root,
        "session",
        ORIGIN,
        transport_is_current=False,
        archive_had_bundle=False,
    )

    assert result.source_format == source_format
    _assert_latest(root, "session", expected, boundary=False)


def test_upgrade_session_recognizes_fully_current_bundle(tmp_path: Path) -> None:
    source = tmp_path / "source" / "session"
    _numeric_session(source, "session", [3], complete_fields=True)
    store = SessionStore(
        StoragePaths(
            source.parent,
            archive=source.parent,
            runtime=tmp_path / "runtime",
            spool=tmp_path / "spool",
        )
    )
    prepared = prepare_generation(store, DirectorySession.load(source / "session.json"))
    extracted = tmp_path / "extracted" / "session"
    extracted.mkdir(parents=True)
    try:
        with prepared.path.open("rb") as archive:
            safe_extract_tar_zst_stream(archive, extracted)
    finally:
        prepared.cleanup()

    with pytest.raises(AlreadyLatest):
        upgrade_session(
            extracted,
            "session",
            ORIGIN,
            transport_is_current=True,
            archive_had_bundle=True,
        )


def _tar_zst(root: Path) -> bytes:
    store = SessionStore(
        StoragePaths(
            root.parent,
            archive=root.parent,
            runtime=root.parent / "archive-runtime",
            spool=root.parent / "archive-spool",
        )
    )
    prepared = prepare_generation(store, DirectorySession.load(root / "session.json"))
    try:
        return prepared.path.read_bytes()
    finally:
        prepared.cleanup()


def _tar_gz(root: Path) -> bytes:
    target = root.parent / "archive.tar.gz"
    with target.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for path in sorted(root.rglob("*")):
                    archive.add(path, arcname=path.relative_to(root), recursive=False)
    return target.read_bytes()


def _put_mutable(
    client: FakeS3,
    config: S3Config,
    archive: bytes,
    *,
    schema: int,
    checkpoint: bool = False,
    compressed: str = "tar.zst",
    step: int = 1,
) -> str:
    base = (
        f"{config.prefix}/user/host/sessions/session"
        if schema == 3
        else f"{config.prefix}/legacy/session"
    )
    directory = "generations" if checkpoint else "steps"
    digest = hashlib.sha256(archive).hexdigest()
    object_key = f"{base}/{directory}/{step}-{digest}.{compressed}"
    latest = f"{base}/latest.json"
    client.objects[object_key] = archive
    client.objects[f"{object_key}.sha256"] = f"{digest}  archive.{compressed}\n".encode()
    pointer: dict[str, object] = {
        "schema_version": schema,
        "session_id": "session",
        "step" if not checkpoint else "generation": step,
        "object": object_key,
        "checksum": f"{object_key}.sha256",
        "digest": digest,
    }
    if schema == 3:
        pointer["origin"] = asdict(ORIGIN)
        client.objects[f"{config.prefix}/index/sessions/session.json"] = json.dumps(
            {
                "schema_version": 1,
                "session_id": "session",
                **asdict(ORIGIN),
                "latest": latest,
            }
        ).encode()
    else:
        pointer["namespace"] = "legacy"
    client.objects[latest] = json.dumps(pointer).encode()
    return object_key


def _put_sidecar(client: FakeS3, config: S3Config, archive: bytes) -> str:
    base = f"{config.prefix}/user/host/sessions/session"
    object_key = f"{base}/generations/00000001.tar.zst"
    checksum_key = f"{base}/generations/00000001.sha256"
    completion_key = f"{base}/completion.json"
    digest = hashlib.sha256(archive).hexdigest()
    client.objects[object_key] = archive
    client.objects[checksum_key] = f"{digest}  00000001.tar.zst\n".encode()
    client.objects[completion_key] = json.dumps(
        {
            "schema_version": 1,
            "session_id": "session",
            "final_step": 1,
            "generation": object_key,
            "sha256": digest,
        }
    ).encode()
    index = f"{config.prefix}/index/sessions/session.json"
    client.objects[index] = json.dumps(
        {
            "schema_version": 1,
            "session_id": "session",
            **asdict(ORIGIN),
        }
    ).encode()
    return object_key


def _put_content_addressed(
    client: FakeS3, config: S3Config, archive: bytes, *, step: int = 1
) -> str:
    session = DirectorySession(
        "session", "/recorded/root", "created", "updated", ORIGIN, state="complete"
    )
    base = remote_sessions._session_base(config, ORIGIN, "session")
    digest = hashlib.sha256(archive).hexdigest()
    object_key = remote_sessions._generation_key(base, step, digest)
    completion = remote_sessions._completion_key(base, step, digest)
    index_key, index_data = remote_sessions._index_record(config, session)
    client.objects[object_key] = archive
    client.objects[completion] = remote_sessions._canonical_json(
        {
            "schema_version": 1,
            "session_id": "session",
            "final_step": step,
            "generation": object_key,
            "sha256": digest,
        }
    )
    client.objects[index_key] = index_data
    return object_key


@pytest.mark.parametrize(
    "layout",
    ["mutable-v1", "mutable-v2", "mutable-v3", "sidecar", "content-addressed"],
)
def test_remote_upgrade_handles_every_transport_and_removes_source_last(
    tmp_path: Path, layout: str
) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    if layout == "mutable-v1":
        _checkpoint_session(source_root, "session")
        original = _put_mutable(
            client,
            config,
            _tar_gz(source_root),
            schema=1,
            checkpoint=True,
            compressed="tar.gz",
            step=2,
        )
    else:
        _numeric_session(source_root, "session", [1, 1])
        archive = _tar_zst(source_root)
        if layout.startswith("mutable-"):
            original = _put_mutable(client, config, archive, schema=int(layout[-1]))
        elif layout == "sidecar":
            original = _put_sidecar(client, config, archive)
        else:
            original = _put_content_addressed(client, config, archive)

    dry_run_objects = dict(client.objects)
    dry_run = recompress_s3(config, client, dry_run=True)
    assert not dry_run.failed
    assert dry_run.migrated == ["session"]
    assert client.objects == dry_run_objects

    summary = recompress_s3(config, client)

    assert not summary.failed
    assert summary.migrated == ["session"]
    assert original not in client.objects
    original_removal = client.operations.index(("remove", original))
    later_source_removals = [
        operation
        for operation in client.operations[original_removal + 1 :]
        if operation[0] == "remove" and "migration-staging" not in operation[1]
    ]
    assert not later_source_removals
    selected_index = remote_sessions._load_index(S3Store(config, client), config, "session")
    selected_base = remote_sessions._session_base(config, selected_index, "session")
    assert remote_sessions._select_generation(
        S3Store(config, client), config, selected_base, "session"
    )[3]


def test_install_preserves_original_when_staging_is_corrupt(tmp_path: Path) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    original = _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    remote = S3Store(config, client)
    source = source_for_candidate(
        remote, config, RemoteCandidate("session", "content-addressed", "session")
    )
    work = tmp_path / "work"
    work.mkdir()
    replacement = _prepare_replacement(remote, source, work)
    original_data = client.objects[original]
    client.corrupt_uploads = True
    try:
        with pytest.raises(ValueError, match="staged replacement"):
            _install_replacement(remote, config, replacement)
    finally:
        replacement.prepared.cleanup()

    assert client.objects[original] == original_data
    assert source.completion_key in client.objects
    assert ("remove", original) not in client.operations


def test_install_preserves_original_when_final_upload_is_corrupt(tmp_path: Path) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    original = _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    remote = S3Store(config, client)
    source = source_for_candidate(
        remote, config, RemoteCandidate("session", "content-addressed", "session")
    )
    work = tmp_path / "work"
    work.mkdir()
    replacement = _prepare_replacement(remote, source, work)
    original_data = client.objects[original]
    completion_data = client.objects[source.completion_key]
    client.corrupt_final_uploads = True
    try:
        with pytest.raises(ValueError, match="uploaded replacement"):
            _install_replacement(remote, config, replacement)
    finally:
        replacement.prepared.cleanup()

    assert client.objects[original] == original_data
    assert client.objects[source.completion_key] == completion_data
    assert ("remove", original) not in client.operations


def test_install_rolls_back_when_replacement_cannot_be_selected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    original = _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    remote = S3Store(config, client)
    source = source_for_candidate(
        remote, config, RemoteCandidate("session", "content-addressed", "session")
    )
    work = tmp_path / "work"
    work.mkdir()
    replacement = _prepare_replacement(remote, source, work)
    original_data = client.objects[original]
    completion_data = client.objects[source.completion_key]
    select = remote_sessions._select_generation
    calls = 0

    def fail_final_selection(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError("injected final selection failure")
        return select(*args, **kwargs)

    monkeypatch.setattr(remote_sessions, "_select_generation", fail_final_selection)
    try:
        with pytest.raises(ValueError, match="injected final selection failure"):
            _install_replacement(remote, config, replacement)
    finally:
        replacement.prepared.cleanup()

    assert client.objects[original] == original_data
    assert client.objects[source.completion_key] == completion_data
    assert ("remove", original) not in client.operations
    assert all(replacement.prepared.digest not in key for key in client.objects if key != original)


def test_independent_snapshot_check_rejects_git_omissions(tmp_path: Path) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    snapshot = source_root / "snapshots" / "0"
    (snapshot / ".gitignore").write_text("hidden.bin\n")
    (snapshot / "hidden.bin").write_bytes(b"must not disappear")
    _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    remote = S3Store(config, client)
    source = source_for_candidate(
        remote, config, RemoteCandidate("session", "content-addressed", "session")
    )
    work = tmp_path / "work"
    work.mkdir()

    with pytest.raises(ValueError, match="filesystem bytes do not match"):
        _prepare_replacement(remote, source, work)


def test_remote_step_must_match_downloaded_archive_head(tmp_path: Path) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    _put_content_addressed(client, config, _tar_zst(source_root), step=1)
    original_objects = dict(client.objects)

    summary = recompress_s3(config, client)

    assert summary.migrated == []
    assert summary.failed == [("session", "archive HEAD 0 does not match selected remote step 1")]
    assert client.objects == original_objects


def test_verified_completion_promotes_embedded_active_session(tmp_path: Path) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    _set_session_state(source_root, "active")
    _put_content_addressed(client, config, _tar_zst(source_root), step=0)

    summary = recompress_s3(config, client, dry_run=True)

    assert summary.migrated == ["session"]
    assert not summary.failed


def test_mutable_pointer_cannot_promote_embedded_active_session(tmp_path: Path) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    _set_session_state(source_root, "active")
    archive = _tar_zst(source_root)
    _put_mutable(client, config, archive, schema=2, step=0)
    original_objects = dict(client.objects)

    summary = recompress_s3(config, client, dry_run=True)

    assert summary.migrated == []
    assert summary.failed == [("session", "remote session is not complete")]
    assert client.objects == original_objects


def test_local_scratch_survives_install_then_is_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    install = s3_recompress._install_replacement
    scratch: Path | None = None

    def inspect_scratch(remote, selected_config, replacement):
        nonlocal scratch
        scratch = replacement.prepared.path.parents[2]
        assert (scratch / ".session.original.tar.zst").is_file()
        install(remote, selected_config, replacement)

    monkeypatch.setattr(s3_recompress, "_install_replacement", inspect_scratch)

    summary = recompress_s3(config, client)

    assert not summary.failed
    assert summary.migrated == ["session"]
    assert scratch is not None
    assert not scratch.exists()


def test_explicit_scratch_parent_contains_only_disposable_run_directories(
    tmp_path: Path,
) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    scratch = tmp_path / "scratch"

    summary = recompress_s3(config, client, dry_run=True, scratch_dir=scratch)

    assert summary.migrated == ["session"]
    assert scratch.is_dir()
    assert list(scratch.iterdir()) == []


def test_default_scratch_uses_user_cache_and_removes_run_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    cache = tmp_path / "cache"
    monkeypatch.setenv("XDG_CACHE_HOME", str(cache))

    summary = recompress_s3(config, client, dry_run=True)

    assert summary.migrated == ["session"]
    scratch = cache / "memo" / "legacy-migrator"
    assert scratch.is_dir()
    assert list(scratch.iterdir()) == []


def test_progress_covers_discovery_download_and_completion(tmp_path: Path) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    updates: list[tuple[int, int, str]] = []

    summary = recompress_s3(
        config,
        client,
        dry_run=True,
        scratch_dir=tmp_path / "scratch",
        progress=lambda completed, total, message: updates.append((completed, total, message)),
    )

    assert summary.migrated == ["session"]
    assert updates[0] == (0, 1, "discovering indexed S3 sessions")
    assert any("downloading source archive" in message for _, _, message in updates)
    assert updates[-1][0] == updates[-1][1]
    assert "finished" in updates[-1][2]


def test_interruption_removes_in_progress_scratch_data(tmp_path: Path) -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    source_root = tmp_path / "source" / "session"
    _numeric_session(source_root, "session", [1])
    _put_content_addressed(client, config, _tar_zst(source_root), step=0)
    scratch = tmp_path / "scratch"

    def interrupt(_completed: int, _total: int, message: str) -> None:
        if "downloading source archive" in message:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        recompress_s3(
            config,
            client,
            dry_run=True,
            scratch_dir=scratch,
            progress=interrupt,
        )

    assert scratch.is_dir()
    assert list(scratch.iterdir()) == []


def test_discovery_rejects_competing_layouts() -> None:
    config = S3Config("bucket", "prefix")
    client = FakeS3()
    client.objects["prefix/index/sessions/session.json"] = b"{}"
    client.objects["prefix/index/sessions/session/" + "a" * 64 + ".json"] = b"{}"

    assert discover_remote_candidates(S3Store(config, client), config) == [
        RemoteCandidate("session", "conflict", "")
    ]
