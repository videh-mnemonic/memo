from __future__ import annotations

import json
import os
import stat
import subprocess
import threading
import time
from pathlib import Path

from memo.cli import main


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
    assert json.loads(exported.read_text())[0]["type"] == "user_input"
    assert main(["--load", "abc123", "--inspect"]) == 0
    inspected = capsys.readouterr().out
    assert "Session: abc123" in inspected
    assert "State: saved" in inspected
    assert "001: complete" in inspected
    assert main(["--load", "abc123", "--traces"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["content"] == "change it"
    assert main(["--load", "abc123", "--traces", "--path", "-"]) == 0
    assert json.loads(capsys.readouterr().out)[0]["content"] == "change it"
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
        assert json.loads(exported.read_text())[0]["content"] == "keep recording"
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
