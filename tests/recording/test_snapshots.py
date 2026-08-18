import os
import subprocess
from pathlib import Path

import pytest

from memo.recording.git_snapshots import GitSnapshotError, GitSnapshotStore
from memo.recording.metadata import DirectorySession, SessionOrigin
from memo.recording.paths import StoragePaths
from memo.recording.snapshots import StepPublisher, scan_tree
from memo.recording.store import SessionStore


def _paths(tmp_path: Path) -> StoragePaths:
    home = tmp_path / "home"
    return StoragePaths(home)


def test_scan_records_policy_size_special_and_deletion(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    git = root / ".git"
    git.mkdir()
    (git / "config").write_text("metadata")
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
    entries = scan_tree(
        root, destination, previous=previous, paths=_paths(tmp_path), max_file_size=3
    )
    by_path = {entry.path: entry for entry in entries}
    assert by_path[".git"].kind == "ignored-policy"
    assert not (destination / ".git").exists()
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
    entry = next(
        item
        for item in scan_tree(root, destination, previous=previous, max_file_size=100)
        if item.path == "changing.txt"
    )
    assert entry.kind == "unstable" and entry.retained
    assert (destination / "changing.txt").read_text() == "prior"


def test_authoritative_scan_captures_changes_without_watcher_hint(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "created.txt").write_text("found")
    destination = tmp_path / "snapshot"
    assert any(
        entry.path == "created.txt" and entry.kind == "file"
        for entry in scan_tree(root, destination, max_file_size=100)
    )


def test_step_publisher_uses_git_commits_and_restores_files(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "unchanged.txt").write_text("same")
    (root / "changed.txt").write_text("one")
    store = SessionStore(_paths(tmp_path))
    session = DirectorySession(
        "session", str(root.resolve()), "now", "now", SessionOrigin("1.0.0", "user", "host")
    )
    store.create(session)
    publisher = StepPublisher(store)

    first = publisher.publish(session)
    (root / "changed.txt").write_text("two")
    second = publisher.publish(session)

    assert first.snapshot_commit
    assert second.snapshot_commit
    assert first.snapshot_commit != second.snapshot_commit
    assert not (store.session_path("session") / "snapshots/0").exists()
    restored = tmp_path / "restored"
    store.restore_manifest("session", second, restored)
    assert (restored / "unchanged.txt").read_text() == "same"
    assert (restored / "changed.txt").read_text() == "two"

    repository = store.session_path("session") / "snapshots.git"
    first_blob = subprocess.run(
        ["git", "--git-dir", str(repository), "ls-tree", first.snapshot_commit, "--", "unchanged.txt"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()[2]
    second_blob = subprocess.run(
        ["git", "--git-dir", str(repository), "ls-tree", second.snapshot_commit, "--", "unchanged.txt"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()[2]
    assert first_blob == second_blob


def test_git_restore_rejects_symlink_entries(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    tree.mkdir()
    os.symlink("/etc/passwd", tree / "escape")
    repository = GitSnapshotStore(tmp_path / "snapshots.git")
    commit = repository.commit(tree, None, "unsafe")

    with pytest.raises(GitSnapshotError, match="unsupported snapshot entry"):
        repository.restore(commit, tmp_path / "restored")
