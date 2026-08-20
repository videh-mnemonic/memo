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
    where = "archive" if args.archive else "local"
    intact = 0
    broken = 0
    # Report each recording as it is decided rather than at the end: reading a
    # whole archive back takes as long as it takes, and a run interrupted
    # partway should still have said what it learned. The bar redraws in place
    # on stderr, so it is closed off before each result rather than being
    # overwritten by it.
    with ProgressBar() as progress:
        position = ""

        def show(completed: int, total: int, message: str) -> None:
            progress.update(completed, total, f"{position} {message}")

        for index, session_id in enumerate(targets, start=1):
            position = f"({index}/{len(targets)})"
            try:
                if args.archive:
                    result = verify_archived_session(
                        session_id, progress=show if progress.enabled else None
                    )
                    detail = f"steps={result['steps']} bytes={result['bytes']}"
                else:
                    assert store is not None
                    show(0, 1, f"verifying {session_id}")
                    head = store.check_integrity(session_id)
                    detail = f"steps={0 if head is None else head.step + 1}"
            except Exception as error:
                broken += 1
                progress.finish()
                print(f"BROKEN: {where}: {session_id}: {error}", file=sys.stderr, flush=True)
            else:
                intact += 1
                progress.finish()
                print(f"intact: {where}: {session_id} {detail}", flush=True)
    print(f"{intact} intact, {broken} broken", flush=True)
    return 1 if broken else 0
