"""Run a shell through a PTY while streaming its terminal activity to Memo."""

from __future__ import annotations

import base64
import errno
import fcntl
import os
import pty
import select
import signal
import termios
import tty
from contextlib import suppress
from pathlib import Path
from types import FrameType

from ..agents.shim import ensure_shims
from ..daemon.client import attach
from ..daemon.protocol import request
from .paths import StoragePaths


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


def run(
    path: Path,
    paths: StoragePaths | None = None,
    shell: str | None = None,
    stdin_fd: int = 0,
    stdout_fd: int = 1,
) -> int:
    paths = paths or StoragePaths.discover()
    shim_directory = ensure_shims(paths)
    allocation = attach(path, paths)
    while allocation.get("decision_required") or allocation.get("stale"):
        if allocation.get("stale"):
            os.write(
                stdout_fd,
                b"The recording changed while awaiting your choice; please choose again.\n",
            )
        prompt = (
            "A Memo recording already exists for this directory.\n\n"
            "1. Resume existing recording\n"
            "2. Start a new recording\n\n"
            "Choice [1]: "
        )
        choice = input(prompt).strip()
        decision = "replace" if choice == "2" else "resume"
        allocation = attach(
            path,
            paths,
            decision=decision,
            expected_session_id=str(allocation["session_id"]),
            expected_revision=int(allocation["revision"]),
        )
    terminal_id = allocation["terminal_id"]
    session_id = allocation["session_id"]
    executable = shell or os.environ.get("SHELL") or "/bin/sh"
    pid, master_fd = pty.fork()
    if pid == 0:
        os.chdir(allocation["root"])
        environment = os.environ.copy()
        environment["MEMO_SESSION_ID"] = session_id
        environment["MEMO_TERMINAL_ID"] = terminal_id
        environment["MEMO_SHIM_DIR"] = str(shim_directory)
        environment["MEMO_RECORDING_ROOT"] = str(Path(allocation["root"]).resolve())
        environment["PATH"] = str(shim_directory) + os.pathsep + environment.get("PATH", "")
        os.execve(executable, [executable], environment)
    sequence = int(allocation["accepted_sequence"])
    original_mode = termios.tcgetattr(stdin_fd) if os.isatty(stdin_fd) else None
    previous_handlers: dict[int, object] = {}

    def forward(signum: int, _frame: FrameType | None) -> None:
        with suppress(ProcessLookupError):
            os.killpg(pid, signum)

    def resize(_signum: int, _frame: FrameType | None) -> None:
        _resize(stdin_fd, master_fd)

    try:
        if original_mode is not None:
            tty.setraw(stdin_fd, termios.TCSANOW)
        _resize(stdin_fd, master_fd)
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP, signal.SIGQUIT):
            previous_handlers[signum] = signal.signal(signum, forward)
        previous_handlers[signal.SIGWINCH] = signal.signal(signal.SIGWINCH, resize)
        input_open = True
        ended = False
        while True:
            readable, _, _ = select.select(
                [master_fd] + ([stdin_fd] if input_open else []), [], [], 0.1
            )
            if stdin_fd in readable:
                data = os.read(stdin_fd, 65536)
                if data:
                    sequence += 1
                    result = request(
                        str(paths.socket),
                        "events",
                        {
                            "session_id": session_id,
                            "terminal_id": terminal_id,
                            "events": [_event(sequence, "input", data)],
                        },
                    )
                    if result.get("recording_ended"):
                        ended = True
                        break
                    os.write(master_fd, data)
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
                sequence += 1
                result = request(
                    str(paths.socket),
                    "events",
                    {
                        "session_id": session_id,
                        "terminal_id": terminal_id,
                        "events": [_event(sequence, "output", data)],
                    },
                )
                if result.get("recording_ended"):
                    ended = True
                    break
                os.write(stdout_fd, data)
            if not readable:
                result = request(
                    str(paths.socket),
                    "events",
                    {
                        "session_id": session_id,
                        "terminal_id": terminal_id,
                        "events": [],
                    },
                )
                if result.get("recording_ended"):
                    ended = True
                    break
        if ended:
            os.write(stdout_fd, b"\r\nmemo: recording ended\r\n")
            with suppress(ProcessLookupError):
                os.killpg(pid, signal.SIGHUP)
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
