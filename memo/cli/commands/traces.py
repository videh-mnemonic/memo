"""List or export terminal and native agent traces for a Memo recording."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ...export import terminal_ids, trace_json, write_traces
from .common import require_local_session

NAME = "traces"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="export agent or terminal traces")
    command.add_argument("session_id")
    command.add_argument("--path", type=Path)
    command.add_argument("--terminals")
    command.add_argument("--list-terminals", action="store_true", help="list terminal stream IDs")
    command.add_argument("--raw", action="store_true", help="export native agent records")
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    require_local_session(args.session_id)
    if args.list_terminals:
        if args.terminals is not None or args.path is not None or args.raw:
            raise ValueError(
                "--list-terminals cannot be combined with --terminals, --path, or --raw"
            )
        values = terminal_ids(args.session_id)
        print("\n".join(values) if values else "No terminal streams.")
        return 0
    selected_terminal_ids = None
    if args.terminals is not None:
        selected_terminal_ids = [value.strip() for value in args.terminals.split(",")]
        if not selected_terminal_ids or any(not value for value in selected_terminal_ids):
            raise ValueError("terminal IDs must be a comma-separated nonempty list")
    if args.path is None or str(args.path) == "-":
        print(trace_json(args.session_id, selected_terminal_ids, raw=args.raw), end="")
    else:
        write_traces(args.session_id, args.path, selected_terminal_ids, raw=args.raw)
    return 0
