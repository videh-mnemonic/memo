"""Pull Memo recordings from remote storage into the local archive."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from ...transport import pull_all_sessions, pull_session
from ..progress import ProgressBar

NAME = "pull"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="pull recordings")
    target = command.add_mutually_exclusive_group(required=True)
    target.add_argument("session_id", nargs="?")
    target.add_argument("--all", action="store_true", dest="all_sessions")
    command.add_argument("--force", action="store_true")
    command.add_argument(
        "--destination",
        type=Path,
        help="install one recording at this exact path instead of Memo's local archive",
    )
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    progress = ProgressBar()
    progress_kwargs = {"progress": progress.update} if progress.enabled else {}
    if args.all_sessions:
        if args.destination is not None:
            raise ValueError("--destination cannot be used with --all")
        with progress:
            summary = pull_all_sessions(force=args.force, **progress_kwargs)
        for session_id in summary.pulled:
            print(f"pulled: {session_id}")
        for session_id in summary.skipped:
            print(f"skipped: local exists: {session_id}")
        for session_id, error in summary.failed:
            print(f"failed: {session_id}: {error}", file=sys.stderr)
        return 1 if summary.failed else 0

    with progress:
        destination = pull_session(
            args.session_id,
            force=args.force,
            destination=args.destination,
            **progress_kwargs,
        )
    print(f"pulled: {args.session_id} path={destination}")
    return 0
