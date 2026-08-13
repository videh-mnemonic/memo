from __future__ import annotations

import base64
import errno
import fcntl
import os
import pty
import select
import signal
import struct
import termios
import tty
from pathlib import Path
from types import FrameType

from .config import Paths
from .daemon import attach
from .protocol import request


def _event(sequence: int, direction: str, data: bytes) -> dict[str, object]:
    return {
        "sequence": sequence,
        "direction": direction,
        "data": base64.b64encode(data).decode("ascii"),
    }


def _resize(source_fd: int, pty_fd: int) -> None:
    try:
        size = fcntl.ioctl(source_fd, termios.TIOCGWINSZ, b"\0" * 8)
        fcntl.ioctl(pty_fd, termios.TIOCSWINSZ, size)
    except OSError:
        pass


def run(path: Path, paths: Paths | None = None, shell: str | None = None,
        stdin_fd: int = 0, stdout_fd: int = 1) -> int:
    paths = paths or Paths.discover()
    allocation = attach(path, paths)
    terminal_id = allocation["terminal_id"]
    session_id = allocation["session_id"]
    assert paths.socket is not None
    executable = shell or os.environ.get("SHELL") or "/bin/sh"
    pid, master_fd = pty.fork()
    if pid == 0:
        os.chdir(allocation["root"])
        os.execv(executable, [executable])
    sequence = int(allocation["accepted_sequence"])
    original_mode = termios.tcgetattr(stdin_fd) if os.isatty(stdin_fd) else None
    previous_handlers: dict[int, object] = {}

    def forward(signum: int, _frame: FrameType | None) -> None:
        try:
            os.killpg(pid, signum)
        except ProcessLookupError:
            pass

    def resize(_signum: int, _frame: FrameType | None) -> None:
        _resize(stdin_fd, master_fd)

    try:
        if original_mode is not None:
            tty.setraw(stdin_fd)
        _resize(stdin_fd, master_fd)
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT):
            previous_handlers[signum] = signal.signal(signum, forward)
        previous_handlers[signal.SIGWINCH] = signal.signal(signal.SIGWINCH, resize)
        input_open = True
        while True:
            readable, _, _ = select.select(
                [master_fd] + ([stdin_fd] if input_open else []), [], [], 0.1
            )
            if stdin_fd in readable:
                data = os.read(stdin_fd, 65536)
                if data:
                    os.write(master_fd, data)
                    sequence += 1
                    request(str(paths.socket), "events", {
                        "session_id": session_id,
                        "terminal_id": terminal_id,
                        "events": [_event(sequence, "input", data)],
                    })
                else:
                    input_open = False
            if master_fd in readable:
                try:
                    data = os.read(master_fd, 65536)
                except OSError as error:
                    if error.errno == errno.EIO:
                        break
                    raise
                if not data:
                    break
                os.write(stdout_fd, data)
                sequence += 1
                request(str(paths.socket), "events", {
                    "session_id": session_id,
                    "terminal_id": terminal_id,
                    "events": [_event(sequence, "output", data)],
                })
        _, status = os.waitpid(pid, 0)
        return os.waitstatus_to_exitcode(status)
    finally:
        try:
            request(str(paths.socket), "detach", {"terminal_id": terminal_id})
        except Exception:
            pass
        if original_mode is not None:
            termios.tcsetattr(stdin_fd, termios.TCSADRAIN, original_mode)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        os.close(master_fd)
