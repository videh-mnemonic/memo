from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from .load import reconstruct, replay, unpack, write_traces
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
    action = result.add_mutually_exclusive_group(required=True)
    action.add_argument("--status", action="store_true", help="list scratch and saved sessions")
    action.add_argument("--save", action="store_true", help="ship eligible scratch sessions")
    action.add_argument("--load", metavar="SESSION_ID", help="load a scratch or shipped session")
    result.add_argument("--all", action="store_true", help="with --save, ship every scratch session")
    result.add_argument("--session", metavar="ID", help="with --save, ship one session")
    result.add_argument("--older-than", default=48.0, type=_hours, metavar="DURATION")
    mode = result.add_mutually_exclusive_group()
    mode.add_argument("--unpack", action="store_true")
    mode.add_argument("--traces", action="store_true")
    mode.add_argument("--replay", action="store_true")
    result.add_argument("--at", choices=["initial", "final"], help="reconstruction point (or use leg:N)")
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
    # argparse choices cannot express leg:N; normalize it after a permissive pre-scan.
    leg_at = None
    if "--at" in argv:
        index = argv.index("--at")
        if index + 1 < len(argv) and argv[index + 1].startswith("leg:"):
            leg_at = argv[index + 1]
            argv[index + 1] = "initial"
    args = parser().parse_args(argv)
    if leg_at:
        args.at = leg_at
    try:
        if args.status:
            print(render_status(), end="")
            return 0
        if args.save:
            return _print_summary(save_sessions(all_sessions=args.all, session_id=args.session,
                                                older_than_hours=args.older_than))
        if args.load:
            if args.unpack:
                print(unpack(args.load))
            elif args.traces:
                if not args.path:
                    raise ValueError("--traces requires --path FILE")
                write_traces(args.load, args.path, args.raw)
            elif args.replay:
                if not args.path or not args.at:
                    raise ValueError("--replay requires --at and --path DIR")
                replay(args.load, args.at, args.path, args.force)
            elif args.at:
                if not args.path:
                    raise ValueError("--at requires --path DIR")
                reconstruct(args.load, args.at, args.path, args.force)
            else:
                raise ValueError("--load requires --unpack, --at, --traces, or --replay")
            return 0
    except Exception as error:
        print(f"memo: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
