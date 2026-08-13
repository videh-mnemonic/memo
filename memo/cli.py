from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from .daemon import activate, end, push
from .load import (inspect_session, reconstruct, replay, terminal_json, trace_json,
                   unpack, write_terminals, write_traces)
from .relay import run as run_relay
from .save import save_sessions
from .status import render_status
from .wrapper import run


def _hours(value: str) -> float:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([mhd]?)", value)
    if not match:
        raise argparse.ArgumentTypeError("use a duration such as 30m, 48h, or 2d")
    amount = float(match.group(1))
    return amount * {"": 1, "m": 1 / 60, "h": 1, "d": 24}[match.group(2)]


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="memo", description="Capture and replay coding-agent sessions")
    action = result.add_mutually_exclusive_group()
    action.add_argument("--status", action="store_true", help="list scratch and saved sessions")
    action.add_argument("--save", action="store_true", help="ship eligible scratch sessions")
    action.add_argument("--load", metavar="SESSION_ID", help="load a scratch or shipped session")
    action.add_argument("--background", action="store_true",
                        help="start or join a directory recording without a terminal")
    action.add_argument("--end", action="store_true", help="finalize a directory recording")
    action.add_argument("--push", action="store_true", help="push changed directory sessions")
    action.add_argument("--pull", metavar="SESSION_ID", help="pull a directory session")
    result.add_argument("recording_path", nargs="?", type=Path,
                        help="directory to record (defaults to the current directory)")
    result.add_argument("--all", action="store_true", help="with --save, ship every scratch session")
    result.add_argument("--session", metavar="ID", help="with --save, ship one session")
    result.add_argument("--older-than", default=48.0, type=_hours, metavar="DURATION")
    mode = result.add_mutually_exclusive_group()
    mode.add_argument("--inspect", action="store_true")
    mode.add_argument("--unpack", action="store_true")
    mode.add_argument("--traces", action="store_true")
    mode.add_argument("--terminals", action="store_true")
    mode.add_argument("--replay", action="store_true")
    result.add_argument("--at", help="reconstruction point, generation:N, or checkpoint:ID")
    result.add_argument("--terminal", metavar="ID", help="select one terminal stream")
    result.add_argument("--path", type=Path, help="output file or directory")
    result.add_argument("--raw", action="store_true", help="preserve raw vendor trace records")
    result.add_argument("--force", action="store_true", help="replace a non-empty reconstruction directory")
    return result


def _print_summary(summary: object) -> int:
    for value in summary.shipped:
        print(f"shipped: {value}")
    for value in summary.locked:
        print(f"skipped: locked: {value}")
    for value in summary.not_idle:
        print(f"skipped: not yet idle: {value}")
    for value, error in summary.failed:
        print(f"failed: {value}: {error}", file=sys.stderr)
    return 1 if summary.failed else 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in {"claude", "codex"}:
        try:
            return run(argv[0], argv[1:])
        except Exception as error:
            print(f"memo: {error}", file=sys.stderr)
            return 1
    args = parser().parse_args(argv)
    try:
        if args.status:
            print(render_status(), end="")
            return 0
        if args.save:
            return _print_summary(save_sessions(all_sessions=args.all, session_id=args.session,
                                                older_than_hours=args.older_than))
        if args.load:
            if args.inspect:
                print(inspect_session(args.load), end="")
            elif args.unpack:
                print(unpack(args.load))
            elif args.traces:
                if not args.path or str(args.path) == "-":
                    print(trace_json(args.load, args.raw), end="")
                else:
                    write_traces(args.load, args.path, args.raw)
            elif args.terminals:
                if not args.path or str(args.path) == "-":
                    print(terminal_json(args.load, args.terminal, args.at), end="")
                else:
                    write_terminals(args.load, args.path, args.terminal, args.at)
            elif args.replay:
                if not args.path or not args.at:
                    raise ValueError("--replay requires --at and --path DIR")
                replay(args.load, args.at, args.path, args.force)
            elif args.at:
                if not args.path:
                    raise ValueError("--at requires --path DIR")
                reconstruct(args.load, args.at, args.path, args.force)
            else:
                raise ValueError("--load requires --inspect, --unpack, --at, --traces, or --replay")
            return 0
        if args.background:
            result = activate(args.recording_path or Path.cwd())
            action = "joined" if result["joined"] else "started"
            print(
                f"{action}: {result['session_id']} "
                f"generation={result['generation']} root={result['root']}"
            )
            return 0
        if args.end:
            result = end(args.recording_path or Path.cwd())
            action = "already complete" if result["already_complete"] else "completed"
            print(f"{action}: {result['session_id']} generation={result['generation']}")
            return 0
        if args.push:
            result = push(args.session)
            for session_id in result["pushed"]:
                print(f"pushed: {session_id}")
            for session_id in result["skipped"]:
                print(f"skipped: unchanged: {session_id}")
            for session_id, error in result["failed"]:
                print(f"failed: {session_id}: {error}", file=sys.stderr)
            return 1 if result["failed"] else 0
        if args.pull:
            from .transport import pull_session
            destination = pull_session(args.pull, force=args.force)
            print(f"pulled: {args.pull} path={destination}")
            return 0
        return run_relay(args.recording_path or Path.cwd())
    except Exception as error:
        print(f"memo: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
