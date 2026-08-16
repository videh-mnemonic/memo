"""Restore a selected step from a Memo recording."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ...export import replay_session
from .common import require_local_session


NAME = "replay"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="restore a recorded step")
    command.add_argument("session_id")
    command.add_argument("at")
    command.add_argument("directory", type=Path)
    command.add_argument("--include-prompts", action="store_true")
    command.add_argument("--force", action="store_true")
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    require_local_session(args.session_id)
    destination = replay_session(
        args.session_id, args.at, args.directory, args.include_prompts, args.force
    )
    print(f"replayed: {args.session_id} step={args.at} path={destination}")
    return 0
