"""Pull a Memo recording from remote storage into the local archive."""

from __future__ import annotations

from typing import Any

from ...transport import pull_session


NAME = "pull"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="pull a recording")
    command.add_argument("session_id")
    command.add_argument("--force", action="store_true")
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    destination = pull_session(args.session_id, force=args.force)
    print(f"pulled: {args.session_id} path={destination}")
    return 0
