from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .daemon import activate, end, push
from .load import inspect_session
from .relay import run as run_relay
from .status import render_status


COMMANDS = {"background", "end", "status", "inspect", "traces", "replay", "push", "pull"}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="memo", description="Record and replay directory sessions")
    subparsers = result.add_subparsers(dest="command")
    record = subparsers.add_parser("record", help=argparse.SUPPRESS)
    record.add_argument("recording_path", nargs="?", type=Path)
    background = subparsers.add_parser("background", help="start or join a recording")
    background.add_argument("path", nargs="?", type=Path)
    finish = subparsers.add_parser("end", help="finish a recording")
    finish.add_argument("path", nargs="?", type=Path)
    subparsers.add_parser("status", help="list recordings")
    inspect = subparsers.add_parser("inspect", help="inspect a recording")
    inspect.add_argument("session_id")
    traces = subparsers.add_parser("traces", help="export terminal traces")
    traces.add_argument("session_id")
    traces.add_argument("--path", type=Path)
    traces.add_argument("--terminals")
    replay = subparsers.add_parser("replay", help="restore a recorded step")
    replay.add_argument("session_id")
    replay.add_argument("at")
    replay.add_argument("directory", type=Path)
    replay.add_argument("--include-prompts", action="store_true")
    replay.add_argument("--force", action="store_true")
    upload = subparsers.add_parser("push", help="push recordings")
    upload.add_argument("session_id", nargs="?")
    download = subparsers.add_parser("pull", help="pull a recording")
    download.add_argument("session_id")
    download.add_argument("--force", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        argv = ["record"]
    elif argv[0] not in COMMANDS and (argv[0] == "." or Path(argv[0]).exists()):
        argv.insert(0, "record")
    args = parser().parse_args(argv)
    try:
        if args.command == "status":
            print(render_status(), end="")
        elif args.command == "inspect":
            print(inspect_session(args.session_id), end="")
        elif args.command == "background":
            response = activate(args.path or Path.cwd())
            action = "joined" if response["joined"] else "started"
            print(f"{action}: {response['session_id']} step={response['step']} root={response['root']}")
        elif args.command == "end":
            response = end(args.path or Path.cwd())
            action = "already complete" if response["already_complete"] else "completed"
            print(f"{action}: {response['session_id']} step={response['step']}")
        elif args.command == "push":
            response = push(args.session_id)
            for session_id in response["pushed"]:
                print(f"pushed: {session_id}")
            for session_id in response["skipped"]:
                print(f"skipped: unchanged: {session_id}")
            for session_id, error in response["failed"]:
                print(f"failed: {session_id}: {error}", file=sys.stderr)
            return 1 if response["failed"] else 0
        elif args.command == "pull":
            from .transport import pull_session
            destination = pull_session(args.session_id, force=args.force)
            print(f"pulled: {args.session_id} path={destination}")
        elif args.command in {"traces", "replay"}:
            raise NotImplementedError(f"{args.command} is not available until Phase 2")
        elif args.command == "record":
            return run_relay(args.recording_path or Path.cwd())
        else:
            parser().error("a command is required")
        return 0
    except Exception as error:
        print(f"memo: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
