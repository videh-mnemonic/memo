from __future__ import annotations

import base64
import json
import tempfile
from pathlib import Path

import pytest

from memo.load import inspect_session, reconstruct, terminal_json, trace_json
from memo.models import CheckpointManifest, DirectorySession, SessionMeta, SnapshotEntry
from memo.session_store import SessionStore, atomic_write
from memo.streams import StreamEvent
from memo.config import Paths


def _paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path, tmp_path / "scratch", tmp_path / "archive", tmp_path / "unpack")


def _fixture(tmp_path: Path) -> tuple[Paths, SessionStore, DirectorySession]:
    root = tmp_path / "root"
    root.mkdir()
    paths = _paths(tmp_path / "home")
    store = SessionStore(paths)
    session = DirectorySession("session", str(root.resolve()), "namespace", "start", "now", "complete")
    session_path = store.create(session)
    for generation, content in ((1, "one"), (2, "two")):
        checkpoint_id = f"checkpoint-{generation}"
        prepared = Path(tempfile.mkdtemp(dir=session_path))
        (prepared / "note.txt").write_text(content)
        manifest = CheckpointManifest(
            checkpoint_id, "session", generation, f"time-{generation}",
            f"snapshots/{checkpoint_id}", [SnapshotEntry("note.txt", "file", 0o644, len(content))],
        )
        store.publish(session, manifest, prepared)
    return paths, store, session


def _agent_fixture(tmp_path: Path) -> Paths:
    paths = _paths(tmp_path / "home")
    session = paths.scratch / "agent-session"
    (session / "traces").mkdir(parents=True)
    SessionMeta(
        session_id="agent-session", provider="claude", repo_kind="synthetic",
        repo_root=str(tmp_path), repo_name="repo", remote="", canonical_remote="",
        archive_namespace="local", initial_head="", final_head="",
        first_seen_utc="start", last_activity_utc="end",
    ).save(session / "meta.json")
    (session / "traces" / "leg-002.jsonl").write_text(
        '{"type":"assistant","content":"done"}\ninvalid\n'
    )
    (session / "traces" / "leg-001.jsonl").write_text(
        '{"type":"user","content":{"text":"fix it"}}\n["native"]\n'
    )
    return paths


def test_directory_inspect_and_checkpoint_reconstruction(tmp_path: Path) -> None:
    paths, _, _ = _fixture(tmp_path)

    output = inspect_session("session", paths)
    assert "Format: directory" in output
    assert "State: complete" in output
    assert "Generation: 2" in output

    historical = reconstruct("session", "generation:1", tmp_path / "historical", paths=paths)
    latest = reconstruct("session", "final", tmp_path / "latest", paths=paths)
    assert (historical / "note.txt").read_text() == "one"
    assert (latest / "note.txt").read_text() == "two"


def test_terminal_export_is_deterministically_merged_and_bounded_by_head(tmp_path: Path) -> None:
    paths, store, session = _fixture(tmp_path)
    session_path = store.session_path("namespace", "session")
    events = [
        StreamEvent("z", 1, "output", base64.b64encode(b"z").decode(), 10),
        StreamEvent("a", 1, "input", base64.b64encode(b"a").decode(), 10),
        StreamEvent("a", 2, "output", base64.b64encode(b"later").decode(), 20),
    ]
    for terminal_id in ("a", "z"):
        terminal = session_path / "streams" / "terminals" / terminal_id
        chunk = terminal / "chunks" / "events.jsonl.gz"
        chunk.parent.mkdir(parents=True)
        import gzip
        values = [event for event in events if event.terminal_id == terminal_id]
        atomic_write(chunk, gzip.compress(b"".join(
            json.dumps(event.to_dict()).encode() + b"\n" for event in values
        ), mtime=0))
        atomic_write(terminal / "stream.json", (json.dumps({
            "schema_version": 1, "terminal_id": terminal_id,
            "highest_sequence": len(values), "chunks": ["chunks/events.jsonl.gz"],
        }) + "\n").encode())
    head = store.head("namespace", "session")
    assert head is not None
    head.stream_high_water = {"a": 1, "z": 1}
    atomic_write(session_path / "checkpoints" / f"{head.checkpoint_id}.json",
                 (json.dumps(head.to_dict()) + "\n").encode())

    exported = json.loads(terminal_json("session", paths=paths))
    assert [(item["terminal_id"], item["data"]) for item in exported] == [("a", "a"), ("z", "z")]


def test_invalid_directory_manifest_is_rejected(tmp_path: Path) -> None:
    paths, store, _ = _fixture(tmp_path)
    session_path = store.session_path("namespace", "session")
    head = store.head("namespace", "session")
    assert head is not None
    (session_path / head.snapshot / "note.txt").unlink()
    with pytest.raises(ValueError, match="missing snapshot file"):
        inspect_session("session", paths)


def test_agent_trace_export_preserves_normal_and_raw_contracts(tmp_path: Path) -> None:
    paths = _agent_fixture(tmp_path)

    normal = json.loads(trace_json("agent-session", paths=paths))
    raw = json.loads(trace_json("agent-session", raw=True, paths=paths))

    assert [(item["position"]["trace"], item["position"]["seq"], item["event"]["type"]) for item in normal] == [
        ("001", 0, "user_input"), ("001", 1, "unknown"),
        ("002", 0, "agent_message"), ("002", 1, "parse_error"),
    ]
    assert normal[0]["event"]["content"] == {"text": "fix it"}
    assert normal[1]["native"]["record"] == ["native"]
    assert raw == [
        {"type": "user", "content": {"text": "fix it"}}, ["native"],
        {"type": "assistant", "content": "done"},
    ]
