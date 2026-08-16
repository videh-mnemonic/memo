from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
import sys
import uuid
from pathlib import Path

from .config import Paths
from .daemon import ensure_daemon
from .agents.harnesses import get_harness, registered_harnesses
from .protocol import request
from .session_store import atomic_write
from .step import utcnow


def ensure_shims(paths: Paths | None = None) -> Path:
    paths = paths or Paths.discover()
    paths.ensure_storage()
    assert paths.runtime is not None
    directory = paths.runtime / "shims"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    names: set[str] = set()
    for harness in registered_harnesses():
        name = harness.executable
        if not name or Path(name).name != name or name in names:
            raise ValueError(f"unsafe or duplicate harness executable: {name}")
        names.add(name)
        body = (
            "#!/bin/sh\n"
            f"exec {shlex.quote(sys.executable)} -m memo.shim {shlex.quote(harness.name)} \"$@\"\n"
        ).encode()
        destination = directory / name
        atomic_write(destination, body)
        destination.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    return directory


def _real_executable(name: str, shim_directory: Path) -> str | None:
    entries = []
    for value in os.environ.get("PATH", "").split(os.pathsep):
        try:
            if Path(value).resolve() == shim_directory.resolve():
                continue
        except OSError:
            pass
        entries.append(value)
    return shutil.which(name, path=os.pathsep.join(entries))


def run(harness_name: str, args: list[str]) -> int:
    harness = get_harness(harness_name)
    shim_directory = Path(os.environ.get("MEMO_SHIM_DIR", ""))
    executable = _real_executable(harness.executable, shim_directory)
    if executable is None:
        print(f"memo: executable not found: {harness.executable}", file=sys.stderr)
        return 127

    session_id = os.environ.get("MEMO_SESSION_ID")
    terminal_id = os.environ.get("MEMO_TERMINAL_ID")
    paths = Paths.discover()
    launch_id = uuid.uuid4().hex
    notified = False
    if session_id and terminal_id:
        try:
            ensure_daemon(paths)
            assert paths.socket is not None
            request(str(paths.socket), "agent_launch", {
                "launch_id": launch_id,
                "session_id": session_id,
                "terminal_id": terminal_id,
                "harness": harness.name,
                "cwd": str(Path.cwd()),
                "command": [harness.executable, *args],
                "started_utc": utcnow(),
            })
            notified = True
        except Exception as error:
            print(f"memo: agent capture unavailable: {error}", file=sys.stderr)

    process = subprocess.Popen([executable, *args], env=os.environ.copy())
    try:
        exit_code = process.wait()
    except KeyboardInterrupt:
        exit_code = process.wait()
    finally:
        if notified:
            try:
                assert paths.socket is not None
                request(str(paths.socket), "agent_complete", {
                    "launch_id": launch_id,
                    "ended_utc": utcnow(),
                    "exit_code": process.returncode,
                }, timeout=60.0)
            except Exception as error:
                print(f"memo: agent completion capture failed: {error}", file=sys.stderr)
    return 128 - exit_code if exit_code < 0 else exit_code


def main() -> int:
    if len(sys.argv) < 2:
        print("memo shim: harness name is required", file=sys.stderr)
        return 2
    return run(sys.argv[1], sys.argv[2:])


if __name__ == "__main__":
    raise SystemExit(main())
