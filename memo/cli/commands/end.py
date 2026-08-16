"""Finish an active Memo recording, with interactive confirmation when needed."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from ...daemon.client import end

NAME = "end"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="finish a recording")
    command.add_argument("path", nargs="?", type=Path)
    command.add_argument("--scope", choices=("partial", "full"))
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    target_path = args.path
    environment_session = os.environ.get("MEMO_SESSION_ID") if target_path is None else None
    environment_terminal = os.environ.get("MEMO_TERMINAL_ID")
    prompt_scope = args.scope is None and sys.stdin.isatty()
    response = end(
        target_path if target_path is not None or environment_session else Path.cwd(),
        session_id=environment_session,
        terminal_id=environment_terminal,
        capture_scope=args.scope,
        prompt_scope=prompt_scope,
    )
    confirmed = False
    expected_revision = None
    while (
        response.get("confirmation_required")
        or response.get("stale")
        or response.get("scope_confirmation_required")
    ):
        if response.get("scope_confirmation_required"):
            answer = input("Did Memo capture all intended work for this session? [y/N] ")
            selected_scope = "full" if answer.strip().lower() in {"y", "yes"} else "partial"
            response = end(
                target_path if target_path is not None or environment_session else Path.cwd(),
                session_id=environment_session,
                terminal_id=environment_terminal,
                confirmed=confirmed,
                expected_revision=expected_revision,
                capture_scope=selected_scope,
            )
            continue
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
        confirmed = True
        expected_revision = int(response["revision"])
        response = end(
            target_path if target_path is not None or environment_session else Path.cwd(),
            session_id=environment_session,
            terminal_id=environment_terminal,
            confirmed=confirmed,
            expected_revision=expected_revision,
            capture_scope=args.scope,
            prompt_scope=prompt_scope,
        )
    action = "already complete" if response["already_complete"] else "completed"
    print(f"{action}: {response['session_id']} step={response['step']}")
    if response.get("cloud") == "pending":
        print("cloud upload started; automatic retry remains enabled", file=sys.stderr)
    return 0
