from __future__ import annotations

import os
import pty
import select
import subprocess
import sys
import termios
import time
from pathlib import Path

from memo.config import Paths
from memo.protocol import request


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
        [sys.executable, "-c", "from memo.relay import run; from pathlib import Path; raise SystemExit(run(Path(r'%s')))" % root],
        stdin=slave, stdout=slave, stderr=slave, env=environment, close_fds=True,
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
        paths = Paths.discover() if os.environ.get("MEMO_HOME") == str(home) else Paths(home)
        if paths.socket and paths.socket.exists():
            request(str(paths.socket), "shutdown")
        os.close(master)
        os.close(slave)
