from __future__ import annotations

from pathlib import Path

import pytest

from memo.registry import OverlappingRootError, Registry


def test_canonical_path_create_persists_and_rejects_duplicate(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    database = tmp_path / "registry.sqlite"
    with Registry(database) as registry:
        first = registry.create(root, "now", "session-one")
        with pytest.raises(RuntimeError, match="already exists"):
            registry.create(root / ".", "later")
    with Registry(database) as registry:
        assert registry.lookup(root) == first


def test_sibling_roots_are_allowed(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    with Registry(tmp_path / "registry.sqlite") as registry:
        registry.create(left, "now")
        registry.create(right, "now")
        assert len(registry.list_active()) == 2


def test_overlapping_roots_are_rejected_without_partial_insert(tmp_path: Path) -> None:
    root = tmp_path / "work"
    child = root / "child"
    child.mkdir(parents=True)
    with Registry(tmp_path / "registry.sqlite") as registry:
        registry.create(root, "now")
        with pytest.raises(OverlappingRootError, match="overlaps"):
            registry.create(child, "now")
        assert len(registry.list_active()) == 1


def test_stale_entries_are_removed(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    with Registry(tmp_path / "registry.sqlite") as registry:
        active = registry.create(root, "now", "stale")
        assert registry.remove_stale(tmp_path / "archive") == [active.session_id]
        assert registry.list_active() == []
