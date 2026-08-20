from __future__ import annotations

import os
import pty
import select
import subprocess
import sys
import termios
import time
from contextlib import suppress
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
    # Touching the recording root makes the daemon publish a step, which is
    # what archives the terminal stream. Ending the recording would too, but
    # that blocks on a real upload this test has no business needing.
    shell.write_text(
        "#!/bin/sh\nread value\nprintf 'reply:%s\\n' \"$value\"\n: > touched\nexit 7\n"
    )
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
        assert (root / "touched").is_file()
        session = next(path for path in home.glob("archive/*") if path.is_dir())
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and not list(session.glob("streams/terminals/*")):
            time.sleep(0.05)
        assert list(session.glob("streams/terminals/*"))
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        paths = StoragePaths(home)
        if paths.socket and paths.socket.exists():
            request(str(paths.socket), "shutdown")
        os.close(master)
        os.close(slave)


def test_shell_survives_a_daemon_that_stops_responding(tmp_path: Path) -> None:
    home = tmp_path / "home"
    root = tmp_path / "root"
    root.mkdir()
    shell = tmp_path / "shell"
    shell.write_text(
        "#!/bin/sh\n"
        "read first\n"
        "printf 'reply:%s\\n' \"$first\"\n"
        "read second\n"
        "printf 'reply:%s\\n' \"$second\"\n"
        "exit 7\n"
    )
    shell.chmod(0o755)
    master, slave = pty.openpty()
    environment = {
        **os.environ,
        "MEMO_HOME": str(home),
        "MEMO_S3_BUCKET": "test-bucket",
        "SHELL": str(shell),
    }
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

    def read_until(needle: bytes, seen: bytearray, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and needle not in seen:
            readable, _, _ = select.select([master], [], [], 0.1)
            if readable:
                seen.extend(os.read(master, 4096))
        return needle in seen

    socket_path = home / "runtime" / "memo.sock"
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not socket_path.exists():
            time.sleep(0.05)
        output = bytearray()
        os.write(master, b"hello\n")
        assert read_until(b"reply:hello", output)

        # Take the daemon away mid-session. Recording cannot continue, but that
        # must not reach through the relay and kill the shell.
        with suppress(Exception):
            request(str(socket_path), "shutdown")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and socket_path.exists():
            time.sleep(0.05)
        socket_path.unlink(missing_ok=True)

        os.write(master, b"world\n")
        assert read_until(b"reply:world", output)
        assert process.wait(timeout=10) == 7
        # The user is told capture stopped, and everything spooled before the
        # daemon went away is still on disk for recovery.
        assert b"no longer being recorded" in output
        assert list(home.glob("runtime/sessions/*/spool/*.frames"))
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        if socket_path.exists():
            with suppress(Exception):
                request(str(socket_path), "shutdown")
        os.close(master)
        os.close(slave)
