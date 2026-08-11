from __future__ import annotations

import getpass
import json
import os
import shutil
import socket
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .config import Paths
from .gitsnap import branch, git_env, head, snapshot_final, snapshot_initial, write_commit_patch
from .identity import discover_repo_identity
from .models import Leg, SessionMeta
from .store import SessionLock
from .tracewatch import locate, mark, session_id


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resume_id(tool: str, args: list[str]) -> str | None:
    for flag in ("--resume", "-r"):
        if flag in args:
            index = args.index(flag)
            if index + 1 < len(args):
                return args[index + 1]
    if tool == "codex" and args[:1] == ["resume"] and len(args) > 1:
        return args[1]
    return None


def _tool_version(tool: str) -> str:
    try:
        return subprocess.run([tool, "--version"], text=True, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _facts(cwd: Path, git_head: str, tool: str) -> dict[str, str]:
    return {"cwd": str(cwd), "git_head": git_head, "host": socket.gethostname(),
            "user": getpass.getuser(), "env_tool_version": _tool_version(tool)}


def _checkpoint_interval() -> float:
    value = os.environ.get("MEMO_CHECKPOINT_INTERVAL", "15")
    try:
        return max(1.0, float(value))
    except ValueError:
        return 15.0


def _copy_trace(trace: Path, session_dir: Path, leg_id: str) -> str:
    trace_name = f"leg-{leg_id}.jsonl"
    trace_dir = session_dir / "traces"
    trace_dir.mkdir(exist_ok=True)
    temporary = trace_dir / f".{trace_name}.tmp"
    shutil.copy2(trace, temporary)
    temporary.replace(trace_dir / trace_name)
    return trace_name


def _adopt_session_id(meta: SessionMeta, paths: Paths, session_dir: Path, leg_id: str,
                      actual_id: str) -> tuple[Path, Path]:
    if actual_id == meta.session_id:
        return session_dir, session_dir / "legs" / leg_id
    target = paths.scratch / actual_id
    if target.exists():
        return session_dir, session_dir / "legs" / leg_id
    session_dir.rename(target)
    meta.session_id = actual_id
    return target, target / "legs" / leg_id


def _new_meta(tool: str, cwd: Path, provisional: str, resumes: str | None = None) -> SessionMeta:
    identity = discover_repo_identity(cwd)
    now = utcnow()
    return SessionMeta(
        session_id=provisional, tool=tool, repo_kind=identity.repo_kind,
        repo_root=str(identity.repo_root), repo_name=identity.repo_name,
        remote=identity.remote, canonical_remote=identity.canonical_remote,
        archive_namespace=identity.archive_namespace, initial_head="", final_head="",
        first_seen_utc=now, last_activity_utc=now, resumes=resumes,
    )


def run(tool: str, args: list[str]) -> int:
    paths = Paths.discover()
    paths.ensure_storage()
    resume = _resume_id(tool, args)
    existing = paths.scratch / resume if resume else None
    if existing and existing.is_dir():
        session_dir = existing
        meta = SessionMeta.load(session_dir / "meta.json")
        if meta.tool != tool:
            raise RuntimeError(f"session {resume} belongs to {meta.tool}, not {tool}")
        is_new = False
    else:
        provisional = f"provisional-{uuid.uuid4().hex}"
        session_dir = paths.scratch / provisional
        session_dir.mkdir(parents=True)
        meta = _new_meta(tool, Path.cwd(), provisional, resume)
        meta.save(session_dir / "meta.json")
        is_new = True

    with SessionLock(session_dir / "session.lock"):
        leg_number = len(meta.legs) + 1
        leg_id = f"{leg_number:03d}"
        leg_dir = session_dir / "legs" / leg_id
        leg_dir.mkdir(parents=True)
        if is_new:
            before = snapshot_initial(meta, session_dir)
            meta.initial_head = before
            meta.final_head = before
            meta.branch = branch(meta, session_dir)
            meta.save(session_dir / "meta.json")
        else:
            before = head(meta, session_dir)
        started = utcnow()
        start = _facts(Path.cwd(), before, tool)
        start["git_head_before"] = start.pop("git_head")
        start["start_utc"] = started
        (leg_dir / "start.json").write_text(json.dumps(start, indent=2) + "\n")
        meta.legs.append(Leg(leg_id, args, started, None, None, None, False))
        meta.last_activity_utc = started
        meta.save(session_dir / "meta.json")
        marker = mark(tool)
        trace_name = None

        def checkpoint() -> None:
            nonlocal session_dir, leg_dir, trace_name
            trace = locate(tool, marker)
            if trace:
                actual_id = session_id(trace)
                if is_new and meta.repo_kind != "synthetic" and actual_id != meta.session_id:
                    session_dir, leg_dir = _adopt_session_id(meta, paths, session_dir, leg_id, actual_id)
                trace_name = _copy_trace(trace, session_dir, leg_id)
            after_checkpoint = snapshot_final(meta, session_dir)
            write_commit_patch(meta, session_dir, before, after_checkpoint, leg_dir / "commits.patch")
            now = utcnow()
            current_leg = meta.legs[-1]
            current_leg.trace_file = trace_name
            meta.final_head = after_checkpoint
            meta.last_activity_utc = now
            meta.save(session_dir / "meta.json")

        try:
            process = subprocess.Popen([tool, *args], env=git_env(meta, session_dir))
            interval = _checkpoint_interval()
            next_checkpoint = time.monotonic()
            while True:
                exit_code = process.poll()
                if exit_code is not None:
                    break
                if time.monotonic() >= next_checkpoint:
                    try:
                        checkpoint()
                    except Exception as error:
                        print(f"memo: checkpoint failed: {error}", file=os.sys.stderr)
                    next_checkpoint = time.monotonic() + interval
                time.sleep(min(0.25, interval))
        except FileNotFoundError:
            exit_code = 127
            print(f"memo: executable not found: {tool}", file=os.sys.stderr)
        except KeyboardInterrupt:
            try:
                process.wait(timeout=2)
                exit_code = process.returncode
            except (NameError, subprocess.TimeoutExpired):
                if "process" in locals():
                    process.terminate()
                    exit_code = process.wait()
                else:
                    exit_code = 130
        trace = locate(tool, marker)
        actual_id = session_id(trace) if trace else meta.session_id.removeprefix("provisional-")
        if is_new and actual_id != meta.session_id:
            session_dir, leg_dir = _adopt_session_id(meta, paths, session_dir, leg_id, actual_id)
        if trace:
            trace_name = _copy_trace(trace, session_dir, leg_id)
        after = snapshot_final(meta, session_dir)
        write_commit_patch(meta, session_dir, before, after, leg_dir / "commits.patch")
        ended = utcnow()
        end = _facts(Path.cwd(), after, tool)
        end["git_head_after"] = end.pop("git_head")
        end["end_utc"] = ended
        end["exit_code"] = exit_code
        (leg_dir / "end.json").write_text(json.dumps(end, indent=2) + "\n")
        meta.legs[-1] = Leg(leg_id, args, started, ended, exit_code, trace_name, True)
        meta.final_head = after
        meta.last_activity_utc = ended
        meta.save(session_dir / "meta.json")
        return exit_code
