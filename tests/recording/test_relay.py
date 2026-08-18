from __future__ import annotations

import os
import pty
import select
import subprocess
import sys
import termios
import time
from pathlib import Path

import pytest

from memo.agents.shim import ensure_shims
from memo.daemon.protocol import request
from memo.recording.paths import StoragePaths
from memo.recording.relay import _shell_argv


def test_bash_startup_cannot_move_provider_binaries_ahead_of_shims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bash = Path("/bin/bash")
    if not bash.exists():
        pytest.skip("bash is unavailable")
    paths = StoragePaths(tmp_path / "memo-home")
    shim_directory = ensure_shims(paths)
    home = tmp_path / "home"
    binaries = home / ".local" / "bin"
    binaries.mkdir(parents=True)
    (binaries / "codex").write_text("#!/bin/sh\nexit 0\n")
    (home / ".bashrc").write_text('export PATH="$HOME/.local/bin:$PATH"\n')
    environment = {
        **os.environ,
        "HOME": str(home),
        "MEMO_SHIM_DIR": str(shim_directory),
        "PATH": f"{shim_directory}{os.pathsep}{binaries}{os.pathsep}/usr/bin",
    }

    completed = subprocess.run(
        [*_shell_argv(str(bash), paths), "-i", "-c", "command -v codex"],
        env=environment,
        text=True,
        capture_output=True,
        check=True,
    )

    assert completed.stdout.strip() == str(shim_directory / "codex")


def test_non_bash_shell_argv_is_unchanged(tmp_path: Path) -> None:
    assert _shell_argv("/bin/zsh", StoragePaths(tmp_path / "home")) == ["/bin/zsh"]


def test_real_pty_relay_is_transparent_and_propagates_exit(tmp_path: Path) -> None:
    home = tmp_path / "home"
    root = tmp_path / "root"
    root.mkdir()
    shell = tmp_path / "shell"
    shell.write_text("#!/bin/sh\nread value\nprintf 'reply:%s\\n' \"$value\"\nexit 7\n")
    shell.chmod(0o755)
    master, slave = pty.openpty()
    original = termios.tcgetattr(slave)
    environment = {**os.environ, "MEMO_HOME": str(home), "SHELL": str(shell)}
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            f"from memo.recording.relay import run; from pathlib import Path; raise SystemExit(run(Path({str(root)!r})))",
        ],
        stdin=slave,
        stdout=slave,
        stderr=slave,
        env=environment,
        close_fds=True,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not list(home.glob("archive/*")):
            time.sleep(0.05)
        os.write(master, b"hello\n")
        output = bytearray()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and b"reply:hello" not in output:
            readable, _, _ = select.select([master], [], [], 0.1)
            if readable:
                output.extend(os.read(master, 4096))
        assert b"reply:hello" in output
        assert process.wait(timeout=5) == 7
        assert termios.tcgetattr(slave) == original
        session = next(path for path in home.glob("archive/*") if path.is_dir())
        socket_path = home / "runtime" / "memo.sock"
        request(str(socket_path), "end", {"path": str(root)})
        assert list(session.glob("streams/terminals/*"))
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        paths = (
            StoragePaths.discover()
            if os.environ.get("MEMO_HOME") == str(home)
            else StoragePaths(home)
        )
        if paths.socket and paths.socket.exists():
            request(str(paths.socket), "shutdown")
        os.close(master)
        os.close(slave)
