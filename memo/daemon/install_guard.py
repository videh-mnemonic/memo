"""Stop the daemon safely before the repository installer replaces Memo."""

from __future__ import annotations

import argparse
import sys

from ..recording.paths import StoragePaths
from .control import stop_daemon


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="prepare a safe Memo upgrade")
    parser.add_argument(
        "--force-stop",
        action="store_true",
        help="stop despite terminals recorded as attached",
    )
    args = parser.parse_args(argv)
    paths = StoragePaths.discover()
    try:
        stopped, attachments = stop_daemon(paths, force=args.force_stop)
    except Exception as error:
        print(f"Memo upgrade refused: {error}", file=sys.stderr)
        return 2
    if attachments:
        print("Memo upgrade refused: recorded terminals are still attached:", file=sys.stderr)
        for attachment in attachments:
            print(
                f"  {attachment.root} ({attachment.session_id}, {attachment.terminal_id})",
                file=sys.stderr,
            )
        print(
            "Close those Memo shells, then rerun ./install. "
            "Use ./install --force-stop only after confirming they are gone.",
            file=sys.stderr,
        )
        return 2
    print("stopped Memo daemon" if stopped else "Memo daemon is not running")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
