"""Push one or all local Memo recordings to remote storage."""

from __future__ import annotations

import sys
from typing import Any

from ...daemon.client import push
from ..progress import ProgressBar

NAME = "push"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="push recordings")
    command.add_argument("session_id", nargs="?")
    command.add_argument("--allow-large", action="store_true")
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    with ProgressBar() as progress:
        progress.update(0, 1, "pushing recordings")
        push_options = {"allow_large": True} if args.allow_large else {}
        response = push(args.session_id, **push_options)
        large_failure = any(
            "exceeding the configured limit" in error for _, error in response["failed"]
        )
        if large_failure and not args.allow_large and sys.stdin.isatty():
            answer = input(
                "A Memo archive exceeds the large-upload safety limit. Upload anyway? [y/N] "
            )
            if answer.strip().lower() in {"y", "yes"}:
                response = push(args.session_id, allow_large=True)
        progress.update(1, 1, "push complete")
    for session_id in response["pushed"]:
        print(f"pushed: {session_id}")
    for session_id in response["skipped"]:
        print(f"skipped: unchanged: {session_id}")
    for session_id, error in response["failed"]:
        print(f"failed: {session_id}: {error}", file=sys.stderr)
    return 1 if response["failed"] else 0
