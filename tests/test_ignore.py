from pathlib import Path

from memo.config import Paths
from memo.ignore import IgnorePolicy


def test_nested_gitignore_memo_override_and_negation(tmp_path: Path) -> None:
    root = tmp_path / "root"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (root / ".gitignore").write_text("*.log\nignored/\n")
    (root / ".memoignore").write_text("!keep.log\n")
    (nested / ".gitignore").write_text("*.tmp\n!important.tmp\n")

    policy = IgnorePolicy(root)
    assert policy.decision(root / "drop.log").ignored
    assert not policy.decision(root / "keep.log").ignored
    assert policy.decision(root / "ignored", is_dir=True).ignored
    assert policy.decision(nested / "drop.tmp").ignored
    assert not policy.decision(nested / "important.tmp").ignored
    assert not policy.decision(nested / "ordinary.txt").ignored


def test_memo_storage_is_excluded_when_nested_in_recording(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    home = root / ".memo-home"
    paths = Paths(home, home / "scratch", home / "archive", tmp_path / "unpack")
    paths.ensure_storage()
    policy = IgnorePolicy(root, paths)

    assert policy.decision(home / "archive", is_dir=True).ignored
    assert not policy.decision(root / "notes.txt").ignored
