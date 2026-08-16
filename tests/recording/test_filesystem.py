from pathlib import Path

import pytest

from memo.recording import filesystem


def test_atomic_write_replaces_content_and_removes_failed_temporary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "state.json"
    filesystem.atomic_write(destination, b"old")
    original_replace = filesystem.os.replace

    def fail_replace(source: str, target: Path) -> None:
        if target == destination:
            raise OSError("injected")
        original_replace(source, target)

    monkeypatch.setattr(filesystem.os, "replace", fail_replace)

    with pytest.raises(OSError, match="injected"):
        filesystem.atomic_write(destination, b"new")

    assert destination.read_bytes() == b"old"
    assert list(tmp_path.iterdir()) == [destination]
