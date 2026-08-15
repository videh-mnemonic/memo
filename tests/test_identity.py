from pathlib import Path

from memo.identity import local_namespace


def test_local_namespaces_use_full_path(tmp_path: Path) -> None:
    one = tmp_path / "one" / "same"
    two = tmp_path / "two" / "same"
    one.mkdir(parents=True)
    two.mkdir(parents=True)
    assert local_namespace(one) != local_namespace(two)
    assert local_namespace(one) == local_namespace(one)
    assert len(local_namespace(one)) <= 120
