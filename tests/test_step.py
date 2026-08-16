import os
from pathlib import Path

from memo.config import StoragePaths
from memo.step import scan_tree


def _paths(tmp_path: Path) -> StoragePaths:
    home = tmp_path / "home"
    return StoragePaths(home)


def test_scan_records_policy_size_special_and_deletion(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / ".gitignore").write_text("ignored.txt\n")
    (root / "ignored.txt").write_text("ignored")
    (root / "large.bin").write_bytes(b"large")
    (root / "kept.txt").write_text("old")
    os.symlink("kept.txt", root / "link")
    previous = tmp_path / "previous"
    previous.mkdir()
    (previous / "kept.txt").write_text("old")
    (previous / "deleted.txt").write_text("gone")
    destination = tmp_path / "snapshot"
    entries = scan_tree(root, destination, previous=previous, paths=_paths(tmp_path), max_file_size=3)
    by_path = {entry.path: entry for entry in entries}
    assert by_path["ignored.txt"].kind == "ignored-policy"
    assert by_path["large.bin"].kind == "oversized"
    assert by_path["link"].kind == "special"
    assert by_path["deleted.txt"].kind == "missing"


def test_unstable_read_retains_prior_version(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "changing.txt").write_text("new")
    previous = tmp_path / "previous"
    previous.mkdir()
    (previous / "changing.txt").write_text("prior")
    destination = tmp_path / "snapshot"
    from memo.recording import snapshots as step
    monkeypatch.setattr(step, "_stable_copy", lambda *args: False)
    entry = next(item for item in scan_tree(root, destination, previous=previous, max_file_size=100)
                 if item.path == "changing.txt")
    assert entry.kind == "unstable" and entry.retained
    assert (destination / "changing.txt").read_text() == "prior"


def test_authoritative_scan_captures_changes_without_watcher_hint(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "created.txt").write_text("found")
    destination = tmp_path / "snapshot"
    assert any(entry.path == "created.txt" and entry.kind == "file"
               for entry in scan_tree(root, destination, max_file_size=100))
