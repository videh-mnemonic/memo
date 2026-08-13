from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from memo.config import Paths
from memo.registry import Registry
from memo.streams import StreamStore, merged_timeline


def _event(sequence: int, data: bytes = b"x") -> dict[str, object]:
    return {"sequence": sequence, "direction": "output",
            "data": base64.b64encode(data).decode()}


def _store(tmp_path: Path) -> tuple[StreamStore, Registry, str]:
    home = tmp_path / "home"
    paths = Paths(home, home / "scratch", home / "archive", tmp_path / "unpack")
    paths.ensure_storage()
    assert paths.registry is not None
    registry = Registry(paths.registry)
    root = tmp_path / "root"
    root.mkdir()
    active, _ = registry.start_or_join(root, "namespace", "now", "session")
    attachment = registry.allocate_attachment(active.session_id, "now", "terminal")
    return StreamStore(paths, registry), registry, attachment.terminal_id


def test_sequence_validation_and_duplicate_rejection(tmp_path: Path) -> None:
    store, registry, terminal_id = _store(tmp_path)
    try:
        assert store.append("session", terminal_id, [_event(1), _event(2)], 10) == 2
        with pytest.raises(ValueError, match="acknowledged sequence"):
            store.append("session", terminal_id, [_event(2)], 11)
        assert [event.sequence for event in store.events("session", terminal_id)] == [1, 2]
    finally:
        registry.close()


def test_sealing_is_immutable_and_reports_high_water(tmp_path: Path) -> None:
    store, registry, terminal_id = _store(tmp_path)
    try:
        session = tmp_path / "home" / "archive" / "namespace" / "session"
        (session / "streams" / "terminals").mkdir(parents=True)
        store.append("session", terminal_id, [_event(1, b"one")], 10)
        assert store.seal_session("namespace", "session") == {terminal_id: 1}
        first = next((session / "streams" / "terminals" / terminal_id / "chunks").iterdir())
        first_bytes = first.read_bytes()
        store.append("session", terminal_id, [_event(2, b"two")], 20)
        assert store.seal_session("namespace", "session") == {terminal_id: 2}
        assert first.read_bytes() == first_bytes
        metadata = json.loads((first.parent.parent / "stream.json").read_text())
        assert metadata["highest_sequence"] == 2
        assert len(metadata["chunks"]) == 2
    finally:
        registry.close()


def test_merged_timeline_has_stable_tie_breakers(tmp_path: Path) -> None:
    store, registry, terminal_id = _store(tmp_path)
    try:
        second = registry.allocate_attachment("session", "now", "alpha")
        session = tmp_path / "home" / "archive" / "namespace" / "session"
        (session / "streams" / "terminals").mkdir(parents=True)
        store.append("session", terminal_id, [_event(1)], 10)
        store.append("session", second.terminal_id, [_event(1)], 10)
        store.seal_session("namespace", "session")
        chunks = session.glob("streams/terminals/*/chunks/*.gz")
        assert [event.terminal_id for event in merged_timeline(chunks)] == ["alpha", "terminal"]
    finally:
        registry.close()
