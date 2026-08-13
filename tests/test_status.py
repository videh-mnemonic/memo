from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path

from memo.config import Paths
from memo.models import SessionMeta
from memo.status import render_status


def _paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path, tmp_path / "scratch", tmp_path / "archive", tmp_path / "unpack")


def _meta(session_id: str, *, shipped: bool = False) -> SessionMeta:
    return SessionMeta(
        session_id=session_id,
        provider="codex",
        repo_kind="real",
        repo_root="/work/example",
        repo_name="example",
        remote="",
        canonical_remote="",
        archive_namespace="local_example",
        initial_head="abc",
        final_head="def",
        first_seen_utc="2026-01-01T00:00:00Z",
        last_activity_utc="2026-01-01T01:00:00Z",
        shipped=shipped,
        shipped_at="2026-01-01T02:00:00Z" if shipped else None,
    )


def _write_archive(path: Path, meta: SessionMeta) -> None:
    path.parent.mkdir(parents=True)
    data = json.dumps(meta.to_dict()).encode()
    info = tarfile.TarInfo("meta.json")
    info.size = len(data)
    with tarfile.open(path, "w:gz") as archive:
        archive.addfile(info, io.BytesIO(data))


def test_status_lists_scratch_and_saved_sessions(tmp_path: Path) -> None:
    configured = _paths(tmp_path)
    scratch = configured.scratch / "scratch-id"
    _meta("scratch-id").save(scratch / "meta.json")
    _write_archive(configured.archive / "local_example" / "saved-id.tar.gz",
                   _meta("saved-id", shipped=True))

    output = render_status(configured)

    assert "scratch  scratch-id" in output
    assert "saved    saved-id" in output


def test_status_shows_full_namespace(tmp_path: Path) -> None:
    configured = _paths(tmp_path)
    namespace = "github.com_videh-mnemonic_example-repository-with-long-name"
    scratch = configured.scratch / "scratch-id"
    meta = _meta("scratch-id")
    meta.archive_namespace = namespace
    meta.save(scratch / "meta.json")

    output = render_status(configured)

    assert namespace in output
    assert "..." not in output


def test_status_reports_no_sessions_when_storage_is_empty(tmp_path: Path) -> None:
    assert render_status(_paths(tmp_path)) == "No sessions.\n"
