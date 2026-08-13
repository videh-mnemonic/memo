from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path

import pytest

from memo.config import Paths
from memo.models import DirectorySession, SessionMeta
from memo.store import AmbiguousSessionError, find_session


def paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path, tmp_path / "scratch", tmp_path / "archive", tmp_path / "unpack")


def _agent_meta(session_id: str, namespace: str = "ns") -> SessionMeta:
    return SessionMeta(
        session_id=session_id, provider="claude", repo_kind="synthetic", repo_root="/tmp",
        repo_name="repo", remote="", canonical_remote="", archive_namespace=namespace,
        initial_head="", final_head="", first_seen_utc="start", last_activity_utc="end",
    )


def _archive(path: Path, meta: SessionMeta) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = (json.dumps(meta.to_dict()) + "\n").encode()
    info = tarfile.TarInfo("meta.json")
    info.size = len(data)
    with tarfile.open(path, "w:gz") as archive:
        archive.addfile(info, io.BytesIO(data))


def test_find_scratch_before_archive(tmp_path: Path) -> None:
    configured = paths(tmp_path)
    _agent_meta("abc").save(configured.scratch / "abc" / "meta.json")
    _archive(configured.archive / "ns" / "abc.tar.gz", _agent_meta("abc"))
    assert find_session("abc", configured).kind == "scratch"


def test_duplicate_archive_is_ambiguous(tmp_path: Path) -> None:
    configured = paths(tmp_path)
    for ns in ("one", "two"):
        _archive(configured.archive / ns / "abc.tar.gz", _agent_meta("abc", ns))
    with pytest.raises(AmbiguousSessionError, match="one, two"):
        find_session("abc", configured)


def test_directory_session_lookup_and_cross_namespace_ambiguity(tmp_path: Path) -> None:
    configured = paths(tmp_path)
    first = configured.archive / "one" / "abc"
    first.mkdir(parents=True)
    (first / "session.json").write_text(json.dumps(
        DirectorySession("abc", "/tmp", "one", "start", "end", "complete").to_dict()
    ))
    location = find_session("abc", configured)
    assert location.kind == "directory"
    assert location.namespace == "one"

    second = configured.archive / "two" / "abc"
    second.mkdir(parents=True)
    (second / "session.json").write_text(json.dumps(
        DirectorySession("abc", "/tmp", "two", "start", "end", "complete").to_dict()
    ))
    with pytest.raises(AmbiguousSessionError, match="one, two"):
        find_session("abc", configured)


def test_lookup_rejects_superseded_agent_metadata(tmp_path: Path) -> None:
    configured = paths(tmp_path)
    scratch = configured.scratch / "abc"
    scratch.mkdir(parents=True)
    (scratch / "meta.json").write_text('{"session_id":"abc","tool":"claude"}\n')
    legacy = configured.archive / "ns" / "abc.tar.gz"
    legacy.parent.mkdir(parents=True)
    data = b'{"session_id":"abc","tool":"claude"}\n'
    info = tarfile.TarInfo("meta.json")
    info.size = len(data)
    with tarfile.open(legacy, "w:gz") as archive:
        archive.addfile(info, io.BytesIO(data))

    with pytest.raises(FileNotFoundError, match="session not found"):
        find_session("abc", configured)
