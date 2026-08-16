"""Import historical native agent sessions into standalone Memo recordings."""

from __future__ import annotations

import sys
from typing import Any

from ...agents.session_import import import_native_sessions

NAME = "import"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="recover native Claude and Codex sessions")
    command.add_argument("--dry-run", action="store_true", help="report without writing recordings")
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    summary = import_native_sessions(dry_run=args.dry_run)
    import_label = "would import" if args.dry_run else "imported"
    refresh_label = "would refresh" if args.dry_run else "refreshed"
    print(f"{import_label}: {len(summary.imported)}")
    print(f"{refresh_label}: {len(summary.refreshed)}")
    print(f"already captured: {len(summary.skipped)}")
    print(f"unimportable: {len(summary.failed)}")
    for session_id in summary.imported:
        print(f"{import_label}: {session_id}")
    for session_id in summary.refreshed:
        print(f"{refresh_label}: {session_id}")
    for source, error in summary.failed:
        print(f"unimportable: {source}: {error}", file=sys.stderr)
    return 1 if summary.failed else 0
