from __future__ import annotations

import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

from .config import Paths
from .daemon import activate
from .harnesses.harness import AgentHarness, source_records
from .protocol import request
from .session_store import atomic_write
from .step import utcnow
from .tracewatch import locate, mark


def _write(path: Path, value: dict[str, object]) -> None:
    atomic_write(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())


def _argument(args: list[str], *flags: str) -> str | None:
    return next((args[index + 1] for index, value in enumerate(args[:-1]) if value in flags), None)


def run(harness: AgentHarness, args: list[str], paths: Paths | None = None) -> int:
    paths = paths or Paths.discover()
    allocation = activate(Path.cwd(), paths)
    session_path = paths.archive / allocation["archive_namespace"] / allocation["session_id"]
    run_id = uuid.uuid4().hex
    metadata_path = session_path / "agents" / "runs" / f"{run_id}.json"
    metadata: dict[str, object] = {
        "run_id": run_id,
        "harness": harness.name,
        "model": _argument(args, "-m", "--model"),
        "reasoning": _argument(args, "--effort"),
        "command": [harness.executable, *args],
        "cwd": str(Path.cwd()),
        "started_utc": utcnow(),
        "ended_utc": None,
        "exit_code": None,
        "agent_session_id": harness.parse_resume(args),
        "trace_file": None,
    }
    _write(metadata_path, metadata)
    roots = harness.trace_roots()
    marker = mark(roots)
    try:
        process = subprocess.Popen([harness.executable, *args], env=os.environ.copy())
        exit_code = process.wait()
    except FileNotFoundError:
        exit_code = 127
        print(f"memo: executable not found: {harness.executable}", file=os.sys.stderr)
    except KeyboardInterrupt:
        process.terminate()
        exit_code = process.wait()

    trace = locate(roots, marker)
    if trace is not None:
        context = next((value | {"effort": record.value.get("effort", value.get("effort"))} for record in source_records(trace) if isinstance(record.value, dict) for value in (record.value.get("payload"), record.value.get("message")) if isinstance(value, dict) and value.get("model")), {})
        metadata.update(model=context.get("model", metadata["model"]), reasoning=context.get("effort", metadata["reasoning"]))
        trace_name = f"{run_id}.jsonl"
        destination = session_path / "agents" / "traces" / trace_name
        temporary = destination.with_name(f".{destination.name}.tmp")
        shutil.copy2(trace, temporary)
        temporary.replace(destination)
        metadata["trace_file"] = trace_name
        metadata["agent_session_id"] = harness.identify_session(source_records(trace), trace)
    metadata["ended_utc"] = utcnow()
    metadata["exit_code"] = exit_code
    _write(metadata_path, metadata)
    assert paths.socket is not None
    request(str(paths.socket), "step", {"path": str(Path.cwd())}, timeout=60.0)
    return exit_code
