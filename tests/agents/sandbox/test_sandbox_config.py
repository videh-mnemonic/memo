from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from memo.agents.sandbox.config import (
    Grant,
    ensure_root_config,
    load_root_config,
    parse_config,
    render_config,
    write_root_config,
)


def test_defaults_initialize_once_and_round_trip(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    path = ensure_root_config(root)
    first = load_root_config(root)
    assert first.network is True
    assert first.gpu is True
    assert first.home_read_write_if_present == (".cache", ".triton", ".nv")

    changed = replace(first, grants=(Grant("/source", "/destination", "read-write"),))
    write_root_config(root, changed)
    ensure_root_config(root)
    assert load_root_config(root) == changed
    assert parse_config(render_config(changed)) == changed
    assert path.read_bytes() == render_config(changed)


def test_unknown_or_malformed_policy_fails_closed() -> None:
    with pytest.raises(ValueError, match="unknown"):
        parse_config(b"network=true\ngpu=true\nmagic=true\n")
    with pytest.raises(ValueError, match="invalid"):
        parse_config(b"network = [")


def test_grant_modes_are_limited() -> None:
    with pytest.raises(ValueError, match="mode"):
        Grant("/a", "/b", "write")
