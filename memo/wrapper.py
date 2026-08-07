from __future__ import annotations

import getpass
import json
import os
import shutil
import socket
import subprocess
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
        marker = mark(tool)
        try:
            process = subprocess.run([tool, *args], env=git_env(meta, session_dir))
            exit_code = process.returncode
        except FileNotFoundError:
            exit_code = 127
            print(f"memo: executable not found: {tool}", file=os.sys.stderr)
        trace = locate(tool, marker)
        actual_id = session_id(trace) if trace else meta.session_id.removeprefix("provisional-")
        if is_new and actual_id != meta.session_id:
            target = paths.scratch / actual_id
            if target.exists():
                raise RuntimeError(f"session already exists: {actual_id}")
            session_dir.rename(target)
            session_dir = target
            leg_dir = session_dir / "legs" / leg_id
            meta.session_id = actual_id
        trace_name = None
        if trace:
            trace_name = f"leg-{leg_id}.jsonl"
            (session_dir / "traces").mkdir(exist_ok=True)
            shutil.copy2(trace, session_dir / "traces" / trace_name)
        after = snapshot_final(meta, session_dir)
        write_commit_patch(meta, session_dir, before, after, leg_dir / "commits.patch")
        ended = utcnow()
        end = _facts(Path.cwd(), after, tool)
        end["git_head_after"] = end.pop("git_head")
        end["end_utc"] = ended
        end["exit_code"] = exit_code
        (leg_dir / "end.json").write_text(json.dumps(end, indent=2) + "\n")
        meta.legs.append(Leg(leg_id, args, started, ended, exit_code, trace_name, True))
        meta.final_head = after
        meta.last_activity_utc = ended
        meta.save(session_dir / "meta.json")
        return exit_code
