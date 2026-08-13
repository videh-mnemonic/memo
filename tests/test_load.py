from __future__ import annotations

import base64
import gzip
import json
import tempfile
from pathlib import Path

import pytest

from memo.config import Paths
from memo.load import inspect_session, replay_session, trace_json, write_traces
from memo.models import DirectorySession, SnapshotEntry, StepManifest
from memo.session_store import SessionStore, atomic_write
from memo.streams import StreamEvent


def _paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path, tmp_path / "scratch", tmp_path / "archive", tmp_path / "unpack")


def _write_stream(session_path: Path, terminal_id: str, events: list[StreamEvent]) -> None:
    terminal = session_path / "streams" / "terminals" / terminal_id
    chunk = terminal / "chunks" / "events.jsonl.gz"
    chunk.parent.mkdir(parents=True)
    atomic_write(chunk, gzip.compress(b"".join(
        json.dumps(event.to_dict()).encode() + b"\n" for event in events
    ), mtime=0))
    atomic_write(terminal / "stream.json", (json.dumps({
        "schema_version": 1,
        "terminal_id": terminal_id,
        "highest_sequence": events[-1].sequence,
        "chunks": ["chunks/events.jsonl.gz"],
    }) + "\n").encode())


def _fixture(tmp_path: Path) -> tuple[Paths, SessionStore, DirectorySession]:
    root = tmp_path / "root"
    root.mkdir()
    paths = _paths(tmp_path / "home")
    store = SessionStore(paths)
    session = DirectorySession("session", str(root.resolve()), "namespace", "start", "now", "complete")
    session_path = store.create(session)
    events = {
        "z": [StreamEvent("z", 1, "output", base64.b64encode(b"z").decode(), 10)],
        "a": [
            StreamEvent("a", 1, "input", base64.b64encode(b"first\n```\n").decode(), 10),
            StreamEvent("a", 2, "input", base64.b64encode(b"later\xff").decode(), 20),
        ],
    }
    for terminal_id, terminal_events in events.items():
        _write_stream(session_path, terminal_id, terminal_events)
    for step, content, high_water in (
        (0, "zero", {"a": 1, "z": 1}),
        (1, "one", {"a": 2, "z": 1}),
    ):
        prepared = Path(tempfile.mkdtemp(dir=session_path))
        (prepared / "note.txt").write_text(content)
        manifest = StepManifest(
            "session", step, f"time-{step}", f"snapshots/{step}",
            [SnapshotEntry("note.txt", "file", 0o644, len(content))], high_water,
        )
        store.publish(session, manifest, prepared)
    return paths, store, session


def test_inspect_reports_latest_step(tmp_path: Path) -> None:
    paths, _, _ = _fixture(tmp_path)
    output = inspect_session("session", paths)
    assert "Format: directory" in output
    assert "State: complete" in output
    assert "Step: 1" in output


def test_trace_export_defaults_to_all_terminals_and_matches_file(tmp_path: Path) -> None:
    paths, _, _ = _fixture(tmp_path)
    output = trace_json("session", paths=paths)
    exported = json.loads(output)
    assert [(item["terminal_id"], item["sequence"]) for item in exported] == [
        ("a", 1), ("z", 1), ("a", 2),
    ]
    assert exported[-1]["data"] == "later\ufffd"
    destination = tmp_path / "exports" / "traces.json"
    assert write_traces("session", destination, paths=paths) == destination
    assert destination.read_text() == output


def test_trace_export_filters_terminals_and_rejects_unknown_ids(tmp_path: Path) -> None:
    paths, _, _ = _fixture(tmp_path)
    exported = json.loads(trace_json("session", ["z"], paths))
    assert [item["terminal_id"] for item in exported] == ["z"]
    with pytest.raises(KeyError, match="terminal stream not found: missing"):
        trace_json("session", ["z", "missing"], paths)


def test_replay_selects_zero_and_latest_and_honors_force(tmp_path: Path) -> None:
    paths, _, _ = _fixture(tmp_path)
    historical = replay_session("session", 0, tmp_path / "historical", paths=paths)
    latest = replay_session("session", -1, tmp_path / "latest", paths=paths)
    assert (historical / "note.txt").read_text() == "zero"
    assert (latest / "note.txt").read_text() == "one"
    with pytest.raises(FileExistsError, match="destination is not empty"):
        replay_session("session", 0, latest, paths=paths)
    replay_session("session", 0, latest, force=True, paths=paths)
    assert (latest / "note.txt").read_text() == "zero"
    with pytest.raises(ValueError, match="invalid step selector"):
        replay_session("session", -2, tmp_path / "invalid", paths=paths)


def test_prompt_output_is_optional_collision_safe_and_manifest_bounded(tmp_path: Path) -> None:
    paths, _, _ = _fixture(tmp_path)
    omitted = replay_session("session", 0, tmp_path / "omitted", paths=paths)
    assert not (omitted / ".prompts.md").exists()
    included = replay_session(
        "session", 0, tmp_path / "included", include_prompts=True, paths=paths
    )
    prompts = (included / ".prompts.md").read_text()
    assert "recorded terminal input events" in prompts
    assert "## Terminal `a`" in prompts
    assert "Sequence 1 | Timestamp" in prompts
    assert "first" in prompts
    assert "````text" in prompts
    assert "later" not in prompts
    latest = replay_session(
        "session", -1, tmp_path / "latest-prompts", include_prompts=True, paths=paths
    )
    assert "later\ufffd" in (latest / ".prompts.md").read_text()


def test_invalid_directory_manifest_is_rejected(tmp_path: Path) -> None:
    paths, store, _ = _fixture(tmp_path)
    session_path = store.session_path("namespace", "session")
    head = store.head("namespace", "session")
    assert head is not None
    (session_path / head.snapshot / "note.txt").unlink()
    with pytest.raises(ValueError, match="missing snapshot file"):
        inspect_session("session", paths)
