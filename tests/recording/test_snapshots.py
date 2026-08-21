import os
import subprocess
from pathlib import Path

import pytest

from memo.recording.git_snapshots import GitSnapshotError, GitSnapshotStore
from memo.recording.metadata import DirectorySession, SessionOrigin, SnapshotEntry, StepManifest
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
    assert first.entries == []
    assert second.entries == []

    repository = store.session_path("session") / "snapshots.git"
    first_blob = subprocess.run(
        [
            "git",
            "--git-dir",
            str(repository),
            "ls-tree",
            first.snapshot_commit,
            "--",
            "unchanged.txt",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()[2]
    second_blob = subprocess.run(
        [
            "git",
            "--git-dir",
            str(repository),
            "ls-tree",
            second.snapshot_commit,
            "--",
            "unchanged.txt",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()[2]
    assert first_blob == second_blob
    assert (
        subprocess.run(
            ["git", "--git-dir", str(repository), "rev-parse", "refs/heads/master"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        == second.snapshot_commit
    )
    subprocess.run(
        ["git", "--git-dir", str(repository), "update-ref", "-d", "refs/heads/master"],
        check=True,
    )
    assert store.head("session").snapshot_commit == second.snapshot_commit


def test_step_publisher_skips_semantic_no_op_and_can_force_boundary(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "untracked.txt").write_text("captured without a project repository")
    store = SessionStore(_paths(tmp_path))
    session = DirectorySession(
        "session", str(root.resolve()), "now", "now", SessionOrigin("1.0.0", "user", "host")
    )
    store.create(session)
    publisher = StepPublisher(store)

    first = publisher.publish(session)
    unchanged = publisher.publish(session)
    boundary = publisher.publish(session, force=True)

    assert unchanged == first
    assert boundary.step == 1
    assert boundary.snapshot_commit != first.snapshot_commit
    repository = GitSnapshotStore(store.session_path("session") / "snapshots.git")
    assert repository.tree_id(boundary.snapshot_commit) == repository.tree_id(first.snapshot_commit)
    assert [manifest.step for manifest in store.steps("session")] == [0, 1]


def test_step_publisher_publishes_stream_metadata_without_tree_change(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = DirectorySession(
        "session", str(root.resolve()), "now", "now", SessionOrigin("1.0.0", "user", "host")
    )
    store.create(session)
    high_water = iter(({"terminal": 0}, {"terminal": 1}))
    publisher = StepPublisher(store, lambda _session: next(high_water))

    first = publisher.publish(session)
    second = publisher.publish(session)

    assert second.step == first.step + 1
    assert second.stream_high_water == {"terminal": 1}
    repository = GitSnapshotStore(store.session_path("session") / "snapshots.git")
    assert repository.tree_id(second.snapshot_commit) == repository.tree_id(first.snapshot_commit)


def test_git_step_manifest_keeps_only_capture_exceptions(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / ".gitignore").write_text("ignored.txt\n")
    (root / "captured.txt").write_text("captured")
    (root / "ignored.txt").write_text("ignored")
    store = SessionStore(_paths(tmp_path))
    session = DirectorySession(
        "session", str(root.resolve()), "now", "now", SessionOrigin("1.0.0", "user", "host")
    )
    store.create(session)

    manifest = StepPublisher(store).publish(session)

    assert [(entry.path, entry.kind) for entry in manifest.entries] == [
        ("ignored.txt", "ignored-policy")
    ]
    assert store.step("session", 0).entries == manifest.entries


def test_compact_manifest_rejects_redundant_entries_but_reads_schema_two() -> None:
    values = {
        "session_id": "session",
        "step": 0,
        "created_utc": "now",
        "snapshot": "snapshots/0",
        "entries": [SnapshotEntry("file.txt", "file", 0o644, 4)],
        "snapshot_commit": "a" * 40,
    }
    StepManifest(**values, schema_version=2).validate()
    # A compact step also carries the digest of the list it stores out of line.
    with pytest.raises(ValueError, match="redundant snapshot entry"):
        StepManifest(**values, schema_version=3, entries_digest="b" * 64).validate()
    without_commit = {**values, "snapshot_commit": None}
    with pytest.raises(ValueError, match="missing its snapshot commit"):
        StepManifest(**without_commit, schema_version=2).validate()


def test_git_snapshot_rejects_symlink_entries_before_storing_them(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    tree.mkdir()
    os.symlink("/etc/passwd", tree / "escape")
    repository = GitSnapshotStore(tmp_path / "snapshots.git")
    with pytest.raises(GitSnapshotError, match="unsupported snapshot entry"):
        repository.commit(tree, None, "unsafe")


def test_git_restore_rejects_imported_symlink_entries(tmp_path: Path) -> None:
    repository = GitSnapshotStore(tmp_path / "snapshots.git")
    repository.initialize()
    blob = (
        subprocess.run(
            ["git", "--git-dir", str(repository.path), "hash-object", "-w", "--stdin"],
            input=b"/etc/passwd",
            capture_output=True,
            check=True,
        )
        .stdout.decode()
        .strip()
    )
    tree = subprocess.run(
        ["git", "--git-dir", str(repository.path), "mktree"],
        input=f"120000 blob {blob}\tescape\n",
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()
    commit = repository.commit_tree(tree, None, "unsafe imported tree")

    with pytest.raises(GitSnapshotError, match="unsupported snapshot entry"):
        repository.restore(commit, tmp_path / "restored")


def test_git_snapshot_stores_ignored_filtered_and_embedded_repository_files(
    tmp_path: Path,
) -> None:
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / ".gitignore").write_text("ignored.txt\n")
    (tree / "ignored.txt").write_bytes(b"ignored but recorded\r\n")
    (tree / ".gitattributes").write_text("*.txt text eol=lf\n")
    nested = tree / "nested"
    nested.mkdir()
    subprocess.run(["git", "init", "--quiet", str(nested)], check=True)
    (nested / "untracked.txt").write_bytes(b"nested bytes\r\n")
    newline = tree / "line\nbreak.bin"
    newline.write_bytes(b"unusual name")
    executable = tree / "executable"
    executable.write_bytes(b"#!/bin/sh\n")
    executable.chmod(0o755)

    repository = GitSnapshotStore(tmp_path / "snapshots.git")
    commit = repository.commit(tree, None, "literal")
    restored = tmp_path / "restored"
    repository.restore(commit, restored)

    expected = {
        path.relative_to(tree).as_posix(): (path.stat().st_mode & 0o111, path.read_bytes())
        for path in tree.rglob("*")
        if path.is_file()
    }
    actual = {
        path.relative_to(restored).as_posix(): (path.stat().st_mode & 0o111, path.read_bytes())
        for path in restored.rglob("*")
        if path.is_file()
    }
    assert actual == expected


def test_git_snapshot_retries_mktree_without_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = tmp_path / "tree"
    nested = tree / "nested"
    nested.mkdir(parents=True)
    (tree / "root.txt").write_bytes(b"root bytes")
    (nested / "child.txt").write_bytes(b"child bytes")
    original = GitSnapshotStore._run_bytes
    batch_attempted = False

    def reject_batch(*arguments, **kwargs):
        nonlocal batch_attempted
        if "mktree" in arguments and "--batch" in arguments:
            batch_attempted = True
            raise GitSnapshotError("injected batched mktree rejection")
        return original(*arguments, **kwargs)

    monkeypatch.setattr(GitSnapshotStore, "_run_bytes", staticmethod(reject_batch))
    repository = GitSnapshotStore(tmp_path / "snapshots.git")
    commit = repository.commit(tree, None, "fallback")
    restored = tmp_path / "restored"
    repository.restore(commit, restored)

    assert batch_attempted
    assert (restored / "root.txt").read_bytes() == b"root bytes"
    assert (restored / "nested" / "child.txt").read_bytes() == b"child bytes"


def test_git_object_recovery_imports_required_unreachable_commit(tmp_path: Path) -> None:
    source = GitSnapshotStore(tmp_path / "source.git")
    first = tmp_path / "first"
    first.mkdir()
    (first / "file").write_text("needed")
    needed = source.commit(first, None, "needed")
    second = tmp_path / "second"
    second.mkdir()
    (second / "file").write_text("published")
    published = source.commit(second, None, "published")

    target = GitSnapshotStore(tmp_path / "target.git")
    assert target.import_objects(source.path, "test", [needed]) == published
    assert target.contains(needed)


def test_git_restore_handles_empty_tree(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    tree.mkdir()
    repository = GitSnapshotStore(tmp_path / "snapshots.git")
    commit = repository.commit(tree, None, "empty")

    destination = tmp_path / "restored"
    repository.restore(commit, destination)

    assert destination.is_dir()
    assert list(destination.iterdir()) == []


def test_snapshot_bundle_is_deterministic_and_uses_requested_commit(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    tree.mkdir()
    repository = GitSnapshotStore(tmp_path / "snapshots.git")
    (tree / "file.txt").write_text("first")
    first = repository.commit(tree, None, "first")
    (tree / "file.txt").write_text("second")
    second = repository.commit(tree, first, "second")
    first_bundle = tmp_path / "first.bundle"
    repeated_bundle = tmp_path / "repeated.bundle"

    repository.create_bundle(first, first_bundle)
    repository.create_bundle(first, repeated_bundle)

    assert first_bundle.read_bytes() == repeated_bundle.read_bytes()
    restored = GitSnapshotStore(tmp_path / "restored.git")
    restored.import_bundle(first_bundle, first)
    assert restored.contains(first)
    assert not restored.contains(second)
    destination = tmp_path / "restored-tree"
    restored.restore(first, destination)
    assert (destination / "file.txt").read_text() == "first"


def test_git_history_queries_are_batched_and_validate_connectivity(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    tree.mkdir()
    repository = GitSnapshotStore(tmp_path / "snapshots.git")
    (tree / "file.txt").write_text("first")
    first = repository.commit(tree, None, "first")
    first_tree = repository.tree_id(first)
    (tree / "file.txt").write_text("second")
    second = repository.commit(tree, first, "second")
    second_tree = repository.tree_id(second)

    assert repository.tree_ids([first, second, first, "f" * 40]) == {
        first: first_tree,
        second: second_tree,
    }
    assert repository.reachable_from(second) == {first, second}
    repository.check_connectivity(second)
