"""Check that recordings are intact locally, or restorable from the archive."""

from __future__ import annotations

import sys
from typing import Any

from ...recording.paths import StoragePaths
from ...recording.store import SessionStore
from ...transport import list_archived_session_ids, verify_archived_session
from ..progress import ProgressBar

NAME = "verify"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="check recordings are intact")
    command.add_argument("session_id", nargs="?")
    command.add_argument(
        "--archive",
        action="store_true",
        help="read each generation back from S3 instead of checking local copies",
    )
    command.set_defaults(handler=run)


def _targets(args: Any) -> list[str]:
    if args.session_id:
        return [str(args.session_id)]
    if args.archive:
        return sorted(list_archived_session_ids())
    return sorted(
        session.session_id for _, session in SessionStore(StoragePaths.discover()).list_sessions()
    )


def run(args: Any) -> int:
    targets = _targets(args)
    if not targets:
        print("no recordings to verify")
        return 0
    store = None if args.archive else SessionStore(StoragePaths.discover())
    intact: list[str] = []
    broken: list[tuple[str, str]] = []
    with ProgressBar() as progress:
        for index, session_id in enumerate(targets):
            progress.update(index, len(targets), f"verifying {session_id}")
            try:
                if args.archive:
                    result = verify_archived_session(session_id)
                    intact.append(f"{session_id} steps={result['steps']} bytes={result['bytes']}")
                else:
                    assert store is not None
                    head = store.check_integrity(session_id)
                    intact.append(f"{session_id} steps={0 if head is None else head.step + 1}")
            except Exception as error:
                broken.append((session_id, str(error)))
        progress.update(len(targets), len(targets), "verify complete")
    where = "archive" if args.archive else "local"
    for line in intact:
        print(f"intact: {where}: {line}")
    for session_id, error in broken:
        print(f"BROKEN: {where}: {session_id}: {error}", file=sys.stderr)
    print(f"{len(intact)} intact, {len(broken)} broken")
    return 1 if broken else 0
