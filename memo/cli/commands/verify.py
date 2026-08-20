"""Check that recordings are intact locally, or restorable from the archive."""

from __future__ import annotations

import signal
import sys
from types import FrameType
from typing import Any

from ...recording.metadata import SessionOrigin
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
    command.add_argument(
        "--all-origins",
        action="store_true",
        help="with --archive, include recordings made on other machines",
    )
    command.add_argument(
        "--limit",
        type=int,
        help="check at most this many recordings",
    )
    command.set_defaults(handler=run)


def _targets(args: Any) -> list[str]:
    if args.session_id:
        return [str(args.session_id)]
    if args.archive:
        # An archive is shared. Reading back every machine's recordings is
        # rarely what someone checking their own archive means, and on a large
        # one it is hours of downloading, so it has to be asked for.
        origin = None if args.all_origins else SessionOrigin.current()
        return sorted(list_archived_session_ids(origin=origin))
    store = SessionStore(StoragePaths.discover())
    return sorted(session.session_id for _, session in store.list_sessions())


def _stop_on_terminate() -> Any:
    """Turn SIGTERM into an exception so in-flight work unwinds.

    Verifying an archive extracts a generation into a temporary directory,
    which can run to tens of gigabytes. Dying on the default handler skips the
    cleanup and strands all of it.
    """

    def terminate(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    return signal.signal(signal.SIGTERM, terminate)


def run(args: Any) -> int:
    if args.limit is not None and args.limit < 1:
        print("--limit must be at least 1", file=sys.stderr)
        return 2
    targets = _targets(args)
    if args.limit is not None:
        targets = targets[: args.limit]
    if not targets:
        print("no recordings to verify")
        return 0
    store = None if args.archive else SessionStore(StoragePaths.discover())
    where = "archive" if args.archive else "local"
    intact = 0
    broken = 0
    previous = _stop_on_terminate()
    # Report each recording as it is decided rather than at the end: reading a
    # whole archive back takes as long as it takes, and a run interrupted
    # partway should still have said what it learned. The bar redraws in place
    # on stderr, so it is closed off before each result rather than being
    # overwritten by it.
    try:
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
                except KeyboardInterrupt:
                    raise
                except Exception as error:
                    broken += 1
                    progress.finish()
                    print(f"BROKEN: {where}: {session_id}: {error}", file=sys.stderr, flush=True)
                else:
                    intact += 1
                    progress.finish()
                    print(f"intact: {where}: {session_id} {detail}", flush=True)
    except KeyboardInterrupt:
        print(f"interrupted after {intact + broken} of {len(targets)}", file=sys.stderr)
        return 130
    finally:
        signal.signal(signal.SIGTERM, previous)
    print(f"{intact} intact, {broken} broken", flush=True)
    return 1 if broken else 0
