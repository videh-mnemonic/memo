from __future__ import annotations

import base64
import json
import threading
from pathlib import Path

import pytest

from memo.config import StoragePaths
from memo.registry import Registry
from memo.streams import StreamStore, merged_timeline


def _event(sequence: int, data: bytes = b"x") -> dict[str, object]:
    return {"sequence": sequence, "direction": "output",
            "data": base64.b64encode(data).decode()}


def _store(tmp_path: Path) -> tuple[StreamStore, Registry, str]:
    home = tmp_path / "home"
    paths = StoragePaths(home)
    paths.ensure_storage()
    assert paths.registry is not None
    registry = Registry(paths.registry)
    root = tmp_path / "root"
    root.mkdir()
    active = registry.create(root, "now", "session")
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
        session = tmp_path / "home" / "archive" / "session"
        (session / "streams" / "terminals").mkdir(parents=True)
        store.append("session", terminal_id, [_event(1, b"one")], 10)
        assert store.seal_session("session") == {terminal_id: 1}
        first = next((session / "streams" / "terminals" / terminal_id / "chunks").iterdir())
        first_bytes = first.read_bytes()
        store.append("session", terminal_id, [_event(2, b"two")], 20)
        assert store.seal_session("session") == {terminal_id: 2}
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
        session = tmp_path / "home" / "archive" / "session"
        (session / "streams" / "terminals").mkdir(parents=True)
        store.append("session", terminal_id, [_event(1)], 10)
        store.append("session", second.terminal_id, [_event(1)], 10)
        store.seal_session("session")
        chunks = session.glob("streams/terminals/*/chunks/*.gz")
        assert [event.terminal_id for event in merged_timeline(chunks)] == ["alpha", "terminal"]
    finally:
        registry.close()


def test_end_drain_waits_for_admitted_event_acknowledgement(
    tmp_path: Path, monkeypatch
) -> None:
    store, registry, terminal_id = _store(tmp_path)
    (tmp_path / "home/archive/session/streams/terminals").mkdir(parents=True)
    reached_ack = threading.Event()
    release_ack = threading.Event()
    original = registry.accept_sequence

    def paused_ack(terminal: str, expected: int, accepted: int) -> None:
        reached_ack.set()
        assert release_ack.wait(2)
        original(terminal, expected, accepted)

    monkeypatch.setattr(registry, "accept_sequence", paused_ack)
    append = threading.Thread(
        target=store.append, args=("session", terminal_id, [_event(1, b"kept")], 10)
    )
    append.start()
    assert reached_ack.wait(2)
    drain = threading.Thread(target=store.drain_and_detach, args=("session", "later"))
    drain.start()
    assert drain.is_alive()
    release_ack.set()
    append.join(2)
    drain.join(2)

    assert registry.attachment(terminal_id).accepted_sequence == 1
    assert registry.attachment(terminal_id).detached_utc == "later"
    assert store.seal_session("session") == {terminal_id: 1}
    assert [event.bytes() for event in store.events("session", terminal_id)] == [b"kept"]
    registry.close()
