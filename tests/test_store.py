from __future__ import annotations

from pathlib import Path

import pytest

from memo.config import Paths
from memo.store import AmbiguousSessionError, find_session


def paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path, tmp_path / "scratch", tmp_path / "archive", tmp_path / "unpack")


def test_find_scratch_before_archive(tmp_path: Path) -> None:
    configured = paths(tmp_path)
    (configured.scratch / "abc").mkdir(parents=True)
    (configured.archive / "ns").mkdir(parents=True)
    (configured.archive / "ns" / "abc.tar.gz").touch()
    assert find_session("abc", configured).kind == "scratch"


def test_duplicate_archive_is_ambiguous(tmp_path: Path) -> None:
    configured = paths(tmp_path)
    for ns in ("one", "two"):
        (configured.archive / ns).mkdir(parents=True)
        (configured.archive / ns / "abc.tar.gz").touch()
    with pytest.raises(AmbiguousSessionError, match="one, two"):
        find_session("abc", configured)

