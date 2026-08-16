from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from .daemon import end, push
from .load import inspect_session, replay_session, trace_json, write_traces
from .relay import run as run_relay
from .status import render_status


COMMANDS = {"end", "status", "inspect", "traces", "replay", "push", "pull"}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="memo", description="Record and replay directory sessions")
    subparsers = result.add_subparsers(dest="command")
    finish = subparsers.add_parser("end", help="finish a recording")
    finish.add_argument("path", nargs="?", type=Path)
    subparsers.add_parser("status", help="list recordings")
    inspect = subparsers.add_parser("inspect", help="inspect a recording")
    inspect.add_argument("session_id")
    traces = subparsers.add_parser("traces", help="export agent or terminal traces")
    traces.add_argument("session_id")
    traces.add_argument("--path", type=Path)
    traces.add_argument("--terminals")
    traces.add_argument("--raw", action="store_true", help="export native agent records")
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
        try:
            return run_relay(Path.cwd())
        except Exception as error:
            print(f"memo: {error}", file=sys.stderr)
            return 1
    elif argv[0] not in COMMANDS and (argv[0] == "." or Path(argv[0]).exists()):
        try:
            return run_relay(Path(argv[0]))
        except Exception as error:
            print(f"memo: {error}", file=sys.stderr)
            return 1
    args = parser().parse_args(argv)
    try:
        if args.command == "status":
            print(render_status(), end="")
        elif args.command == "inspect":
            print(inspect_session(args.session_id), end="")
        elif args.command == "end":
            target_path = args.path
            environment_session = os.environ.get("MEMO_SESSION_ID") if target_path is None else None
            environment_terminal = os.environ.get("MEMO_TERMINAL_ID")
            response = end(
                target_path if target_path is not None or environment_session else Path.cwd(),
                session_id=environment_session,
                terminal_id=environment_terminal,
            )
            while response.get("confirmation_required") or response.get("stale"):
                count = int(response["other_terminals"])
                if response.get("stale"):
                    print("The recording's attached terminals changed; confirmation is required again.")
                answer = input(
                    f"This recording has {count} other attached terminal"
                    f"{'s' if count != 1 else ''}.\n"
                    "End the recording for all terminals? [y/N] "
                )
                if answer.strip().lower() not in {"y", "yes"}:
                    print("recording unchanged")
                    return 0
                response = end(
                    target_path if target_path is not None or environment_session else Path.cwd(),
                    session_id=environment_session,
                    terminal_id=environment_terminal,
                    confirmed=True,
                    expected_revision=int(response["revision"]),
                )
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
        elif args.command == "traces":
            terminal_ids = None
            if args.terminals is not None:
                terminal_ids = [value.strip() for value in args.terminals.split(",")]
                if not terminal_ids or any(not value for value in terminal_ids):
                    raise ValueError("terminal IDs must be a comma-separated nonempty list")
            if args.path is None or str(args.path) == "-":
                print(trace_json(args.session_id, terminal_ids, raw=args.raw), end="")
            else:
                write_traces(args.session_id, args.path, terminal_ids, raw=args.raw)
        elif args.command == "replay":
            destination = replay_session(
                args.session_id, args.at, args.directory, args.include_prompts, args.force
            )
            print(f"replayed: {args.session_id} step={args.at} path={destination}")
        else:
            parser().error("a command is required")
        return 0
    except Exception as error:
        print(f"memo: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
