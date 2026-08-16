from pathlib import Path

from memo.recording.paths import StoragePaths
from memo.recording.ignore import IgnorePolicy


def test_nested_gitignore_and_negation_apply_within_repository(tmp_path: Path) -> None:
    root = tmp_path / "root"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / ".git").mkdir()
    (root / ".gitignore").write_text("*.log\nignored/\n")
    (nested / ".gitignore").write_text("*.tmp\n!important.tmp\n")

    policy = IgnorePolicy(root)
    assert policy.decision(root / "drop.log").ignored
    assert policy.decision(root / "ignored", is_dir=True).ignored
    assert policy.decision(nested / "drop.tmp").ignored
    assert not policy.decision(nested / "important.tmp").ignored
    assert not policy.decision(nested / "ordinary.txt").ignored


def test_nested_git_directory_resets_outer_rules(tmp_path: Path) -> None:
    root = tmp_path / "root"
    repository = root / "nested"
    repository.mkdir(parents=True)
    (root / ".git").mkdir()
    (root / ".gitignore").write_text("*.log\nnested/\n")
    (repository / ".git").mkdir()
    (repository / ".gitignore").write_text("*.tmp\n")

    policy = IgnorePolicy(root)
    assert not policy.decision(repository, is_dir=True).ignored
    assert not policy.decision(repository / "kept.log").ignored
    assert policy.decision(repository / "drop.tmp").ignored


def test_nested_git_file_resets_outer_rules(tmp_path: Path) -> None:
    root = tmp_path / "root"
    worktree = root / "worktree"
    worktree.mkdir(parents=True)
    (root / ".git").mkdir()
    (root / ".gitignore").write_text("*.log\n")
    (worktree / ".git").write_text("gitdir: ../git/worktrees/worktree\n")
    (worktree / ".gitignore").write_text("*.cache\n")

    policy = IgnorePolicy(root)
    assert not policy.decision(worktree / "kept.log").ignored
    assert policy.decision(worktree / "drop.cache").ignored


def test_memo_storage_is_excluded_when_nested_in_recording(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    home = root / ".memo-home"
    paths = StoragePaths(home)
    paths.ensure_storage()
    policy = IgnorePolicy(root, paths)

    assert policy.decision(home / "archive", is_dir=True).ignored
    assert policy.decision(home / "runtime", is_dir=True).ignored
    assert not policy.decision(root / "notes.txt").ignored
