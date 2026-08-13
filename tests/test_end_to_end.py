from __future__ import annotations

import json
import os
import stat
import subprocess
import pty
import select
import sys
import threading
import time
from pathlib import Path

import pytest

from memo.cli import main
from memo.config import Paths
from memo.protocol import request
from memo.session_store import SessionStore


STUB = '''#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
if "--version" in sys.argv:
    print("stub 1.0")
    raise SystemExit(0)
root = pathlib.Path(os.environ["STUB_ROOT"])
(root / "tracked.txt").write_text("final tracked\\n")
(root / "untracked.bin").write_bytes(b"\\x00memo\\xff")
subprocess.run(["git", "add", "tracked.txt"], cwd=root, check=True)
subprocess.run(["git", "commit", "-m", "agent change"], cwd=root, check=True,
               stdout=subprocess.DEVNULL)
trace = pathlib.Path(os.environ["MEMO_TRACE_DIR"])
trace.mkdir(parents=True, exist_ok=True)
with (trace / "session-abc.jsonl").open("w") as f:
    f.write(json.dumps({"session_id":"abc123", "type":"user", "timestamp":"2026-01-01T00:00:00Z", "content":"change it"}) + "\\n")
    f.write(json.dumps({"type":"assistant", "content":"done"}) + "\\n")
'''


LIVE_STUB = '''#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys, time
if "--version" in sys.argv:
    print("stub 1.0")
    raise SystemExit(0)
root = pathlib.Path(os.environ["STUB_ROOT"])
trace = pathlib.Path(os.environ["MEMO_TRACE_DIR"])
trace.mkdir(parents=True, exist_ok=True)
with (trace / "session-live.jsonl").open("w") as f:
    f.write(json.dumps({"session_id":"live123", "type":"user", "timestamp":"2026-01-01T00:00:00Z", "content":"keep recording"}) + "\\n")
    f.flush()
    (root / "tracked.txt").write_text("live tracked\\n")
    subprocess.run(["git", "add", "tracked.txt"], cwd=root, check=True)
    subprocess.run(["git", "commit", "-m", "live change"], cwd=root, check=True,
                   stdout=subprocess.DEVNULL)
    time.sleep(3)
    f.write(json.dumps({"type":"assistant", "content":"still running"}) + "\\n")
'''


def _tree(path: Path) -> dict[str, bytes]:
    return {str(p.relative_to(path)): p.read_bytes() for p in path.rglob("*") if p.is_file() and ".git" not in p.parts}


def test_real_repo_capture_save_load(tmp_path: Path, monkeypatch, capsys) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", str(repo)], check=True, stdout=subprocess.PIPE)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("initial\n")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=repo, check=True, stdout=subprocess.PIPE)
    binary = tmp_path / "bin"
    binary.mkdir()
    stub = binary / "claude"
    stub.write_text(STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    memo_home = tmp_path / "memo-home"
    monkeypatch.setenv("MEMO_HOME", str(memo_home))
    monkeypatch.setenv("MEMO_TRACE_DIR", str(tmp_path / "traces"))
    monkeypatch.setenv("STUB_ROOT", str(repo))
    monkeypatch.setenv("PATH", f"{binary}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.chdir(repo)

    assert main(["claude"]) == 0
    meta = json.loads((memo_home / "scratch" / "abc123" / "meta.json").read_text())
    assert meta["format"] == "memo-agent-session"
    assert meta["format_version"] == 1
    assert meta["provider"] == "claude"
    assert meta["archive_namespace"].startswith("local_repo_")
    assert main(["--save", "--all"]) == 0
    archive = memo_home / "archive" / meta["archive_namespace"] / "abc123.tar.gz"
    assert archive.is_file()
    assert archive.with_suffix(".gz.sha256").is_file()
    assert not (memo_home / "scratch" / "abc123").exists()

    initial = tmp_path / "initial-restored"
    assert main(["--load", "abc123", "--at", "initial", "--path", str(initial)]) == 0
    assert (initial / "tracked.txt").read_text() == "initial\n"
    assert not (initial / "untracked.bin").exists()
    through_leg = tmp_path / "leg-restored"
    assert main(["--load", "abc123", "--at", "leg:1", "--path", str(through_leg)]) == 0
    assert (through_leg / "tracked.txt").read_text() == "final tracked\n"

    restored = tmp_path / "restored"
    assert main(["--load", "abc123", "--at", "final", "--path", str(restored)]) == 0
    assert _tree(restored) == _tree(repo)
    exported = tmp_path / "normalized.json"
    assert main(["--load", "abc123", "--traces", "--path", str(exported)]) == 0
    first_event = json.loads(exported.read_text())[0]
    assert first_event["schema_version"] == 1
    assert first_event["provider"] == "claude"
    assert first_event["event"]["type"] == "user_input"
    assert first_event["native"]["record"]["session_id"] == "abc123"
    assert main(["--load", "abc123", "--inspect"]) == 0
    inspected = capsys.readouterr().out
    assert "Session: abc123" in inspected
    assert "State: saved" in inspected
    assert "001: complete" in inspected
    assert main(["--load", "abc123", "--traces"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["event"]["content"] == "change it"
    assert main(["--load", "abc123", "--traces", "--path", "-"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["event"]["content"] == "change it"
    replayed = tmp_path / "replayed"
    assert main(["--load", "abc123", "--replay", "--at", "final", "--path", str(replayed)]) == 0
    assert "change it" in (replayed / "MEMO_TASK.md").read_text()


def test_synthetic_does_not_create_dot_git(tmp_path: Path, monkeypatch) -> None:
    work = tmp_path / "plain"
    work.mkdir()
    (work / "tracked.txt").write_text("initial\n")
    binary = tmp_path / "bin"
    binary.mkdir()
    stub = binary / "codex"
    stub.write_text(STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("MEMO_HOME", str(tmp_path / "memo-home"))
    monkeypatch.setenv("MEMO_TRACE_DIR", str(tmp_path / "traces"))
    monkeypatch.setenv("STUB_ROOT", str(work))
    monkeypatch.setenv("PATH", f"{binary}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.chdir(work)
    assert main(["codex"]) == 0
    meta = next((tmp_path / "memo-home" / "scratch").glob("*/meta.json"))
    assert json.loads(meta.read_text())["provider"] == "codex"
    assert not (work / ".git").exists()
    assert main(["--save", "--all"]) == 0
    restored = tmp_path / "synthetic-restored"
    assert main(["--load", "abc123", "--at", "final", "--path", str(restored)]) == 0
    assert _tree(restored) == _tree(work)


def test_live_session_checkpoints_before_exit(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", str(repo)], check=True, stdout=subprocess.PIPE)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("initial\n")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=repo, check=True, stdout=subprocess.PIPE)
    binary = tmp_path / "bin"
    binary.mkdir()
    stub = binary / "codex"
    stub.write_text(LIVE_STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    memo_home = tmp_path / "memo-home"
    monkeypatch.setenv("MEMO_HOME", str(memo_home))
    monkeypatch.setenv("MEMO_TRACE_DIR", str(tmp_path / "traces"))
    monkeypatch.setenv("MEMO_CHECKPOINT_INTERVAL", "1")
    monkeypatch.setenv("STUB_ROOT", str(repo))
    monkeypatch.setenv("PATH", f"{binary}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.chdir(repo)

    result = {}
    thread = threading.Thread(target=lambda: result.setdefault("code", main(["codex"])))
    thread.start()
    try:
        session = memo_home / "scratch" / "live123"
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not (session / "traces" / "leg-001.jsonl").is_file():
            time.sleep(0.1)

        assert thread.is_alive()
        meta = json.loads((session / "meta.json").read_text())
        assert meta["legs"][0]["complete"] is False
        assert meta["legs"][0]["trace_file"] == "leg-001.jsonl"
        assert "keep recording" in (session / "traces" / "leg-001.jsonl").read_text()
        assert (session / "legs" / "001" / "commits.patch").is_file()
        exported = tmp_path / "live-traces.json"
        assert main(["--load", "live123", "--traces", "--path", str(exported)]) == 0
        event = json.loads(exported.read_text())[0]
        assert event["provider"] == "codex"
        assert event["event"]["content"] == "keep recording"
        restored = tmp_path / "live-restored"
        assert main(["--load", "live123", "--at", "final", "--path", str(restored)]) == 0
        assert (restored / "tracked.txt").read_text() == "live tracked\n"
    finally:
        thread.join(timeout=10)
    assert result == {"code": 0}


def test_live_session_id_collision_keeps_provisional_session(tmp_path: Path, monkeypatch, capsys) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", str(repo)], check=True, stdout=subprocess.PIPE)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    (repo / "tracked.txt").write_text("initial\n")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=repo, check=True, stdout=subprocess.PIPE)
    binary = tmp_path / "bin"
    binary.mkdir()
    stub = binary / "codex"
    stub.write_text(LIVE_STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    memo_home = tmp_path / "memo-home"
    (memo_home / "scratch" / "live123").mkdir(parents=True)
    monkeypatch.setenv("MEMO_HOME", str(memo_home))
    monkeypatch.setenv("MEMO_TRACE_DIR", str(tmp_path / "traces"))
    monkeypatch.setenv("MEMO_CHECKPOINT_INTERVAL", "1")
    monkeypatch.setenv("STUB_ROOT", str(repo))
    monkeypatch.setenv("PATH", f"{binary}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.chdir(repo)

    assert main(["codex"]) == 0
    assert "checkpoint failed" not in capsys.readouterr().err
    provisional = [path for path in (memo_home / "scratch").iterdir() if path.name.startswith("provisional-")]
    assert len(provisional) == 1
    meta = json.loads((provisional[0] / "meta.json").read_text())
    assert meta["legs"][0]["complete"] is True
    assert meta["legs"][0]["trace_file"] == "leg-001.jsonl"


def test_public_cli_starts_background_directory_recording(tmp_path: Path, monkeypatch, capsys) -> None:
    home = tmp_path / "memo-home"
    root = tmp_path / "plain-directory"
    root.mkdir()
    (root / "notes.txt").write_text("initial\n")
    monkeypatch.setenv("MEMO_HOME", str(home))
    monkeypatch.setenv("MEMO_CHECKPOINT_INTERVAL", "1")

    assert main(["--background", str(root)]) == 0
    output = capsys.readouterr().out
    assert "started:" in output
    paths = Paths.discover()
    sessions = list((home / "archive").glob("*/*/session.json"))
    assert len(sessions) == 1
    session_dir = sessions[0].parent
    store = SessionStore(paths)
    head = store.head(session_dir.parent.name, session_dir.name)
    assert head is not None
    assert (session_dir / head.snapshot / "notes.txt").read_text() == "initial\n"

    assert main(["--background", str(root)]) == 0
    assert "joined:" in capsys.readouterr().out
    assert paths.socket is not None
    request(str(paths.socket), "shutdown")


def test_two_interactive_attachments_publish_one_directory_checkpoint(tmp_path: Path) -> None:
    home = tmp_path / "memo-home"
    root = tmp_path / "work"
    root.mkdir()
    shell = tmp_path / "shell"
    shell.write_text(
        "#!/bin/sh\nread name\nprintf '%s\\n' \"$name\" > \"$name.txt\"\nprintf 'done:%s\\n' \"$name\"\n"
    )
    shell.chmod(0o755)
    environment = {**os.environ, "MEMO_HOME": str(home), "SHELL": str(shell),
                   "MEMO_CHECKPOINT_INTERVAL": "1"}
    processes = []
    masters = []
    try:
        for name in ("one", "two"):
            master, slave = pty.openpty()
            process = subprocess.Popen(
                [sys.executable, "-m", "memo.cli", str(root)], stdin=slave, stdout=slave,
                stderr=slave, env=environment, close_fds=True,
            )
            os.close(slave)
            processes.append(process)
            masters.append(master)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not list(home.glob("archive/*/*/session.json")):
                time.sleep(0.05)
            time.sleep(0.1)
            os.write(master, f"{name}\n".encode())
        for name, process, master in zip(("one", "two"), processes, masters):
            output = bytearray()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and f"done:{name}".encode() not in output:
                readable, _, _ = select.select([master], [], [], 0.1)
                if readable:
                    try:
                        output.extend(os.read(master, 4096))
                    except OSError:
                        break
            assert f"done:{name}".encode() in output
            assert process.wait(timeout=5) == 0
        paths = Paths(home, home / "scratch", home / "archive", tmp_path / "unpack")
        assert paths.socket is not None
        request(str(paths.socket), "checkpoint", {"path": str(root)})
        session = next(home.glob("archive/*/*"))
        manifest = SessionStore(paths).head(session.parent.name, session.name)
        assert manifest is not None
        assert len(manifest.stream_high_water) == 2
        assert (session / manifest.snapshot / "one.txt").read_text().strip() == "one"
        assert (session / manifest.snapshot / "two.txt").read_text().strip() == "two"
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
        socket_path = home / "runtime" / "memo.sock"
        if socket_path.exists():
            request(str(socket_path), "shutdown")
        for master in masters:
            os.close(master)


def test_end_restore_export_and_restart_directory_session(tmp_path: Path, monkeypatch, capsys) -> None:
    home = tmp_path / "memo-home"
    root = tmp_path / "work"
    root.mkdir()
    (root / "note.txt").write_text("recorded\n")
    monkeypatch.setenv("MEMO_HOME", str(home))
    monkeypatch.setenv("MEMO_CHECKPOINT_INTERVAL", "60")

    assert main(["--background", str(root)]) == 0
    first_session = next(home.glob("archive/*/*/session.json")).parent
    assert main(["--end", str(root)]) == 0
    assert "completed:" in capsys.readouterr().out
    first_meta = json.loads((first_session / "session.json").read_text())
    assert first_meta["state"] == "complete"
    paths = Paths.discover()
    assert paths.registry is not None
    from memo.registry import Registry
    with Registry(paths.registry) as registry:
        assert registry.lookup(root) is None

    restored = tmp_path / "restored"
    assert main(["--load", first_session.name, "--at", "final", "--path", str(restored)]) == 0
    assert (restored / "note.txt").read_text() == "recorded\n"
    assert main(["--load", first_session.name, "--terminals"]) == 0
    assert json.loads(capsys.readouterr().out) == []

    assert main(["--background", str(root)]) == 0
    sessions = [path.parent for path in home.glob("archive/*/*/session.json")]
    assert len(sessions) == 2
    assert {path.name for path in sessions} != {first_session.name}
    assert paths.socket is not None
    request(str(paths.socket), "shutdown")


def test_push_pull_and_restore_directory_generation(tmp_path: Path) -> None:
    from test_transport import FakeS3, _paths, _published
    from memo.config import TransportConfig
    from memo.transport import pull_session, push_session

    root = tmp_path / "source"
    root.mkdir()
    source_paths = _paths(tmp_path / "source-home")
    store, session = _published(source_paths, root)
    client = FakeS3()
    config = TransportConfig("bucket", "e2e")
    assert push_session(store, session, config, client)["status"] == "pushed"

    destination_paths = _paths(tmp_path / "destination-home")
    pull_session("session", destination_paths, config, client=client)
    restored = tmp_path / "restored"
    destination_store = SessionStore(destination_paths)
    destination_store.restore("namespace", "session", restored)
    assert (restored / "file.txt").read_text() == "generation 1\n"

    (restored / "local-only.txt").write_text("preserved")
    with pytest.raises(FileExistsError):
        pull_session("session", destination_paths, config, client=client)
    assert (restored / "local-only.txt").read_text() == "preserved"
