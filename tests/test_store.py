from __future__ import annotations

import json
from pathlib import Path

import pytest

from memo.config import Paths
from memo.models import DirectorySession
from memo.store import AmbiguousSessionError, find_session


def _write_session(paths: Paths, namespace: str, session_id: str, root: Path) -> Path:
    directory = paths.archive / namespace / session_id
    directory.mkdir(parents=True)
    session = DirectorySession(
        session_id,
        str(root.resolve()),
        namespace,
        "now",
        "now",
    )
    (directory / "session.json").write_text(json.dumps(session.to_dict()))
    return directory


def test_directory_session_lookup(tmp_path: Path) -> None:
    paths = Paths(tmp_path / "home")
    directory = _write_session(paths, "one", "abc", tmp_path)

    location, session = find_session("abc", paths)

    assert location == directory
    assert session.session_id == "abc"
    assert session.archive_namespace == "one"


def test_directory_session_lookup_is_ambiguous_across_namespaces(tmp_path: Path) -> None:
    paths = Paths(tmp_path / "home")
    _write_session(paths, "one", "abc", tmp_path)
    _write_session(paths, "two", "abc", tmp_path)

    with pytest.raises(AmbiguousSessionError, match="one, two"):
        find_session("abc", paths)
