"""Migrate recordings created by the pre-daemon Memo prototype."""

from __future__ import annotations

import sys
from typing import Any

from ...legacy import migrate_legacy

NAME = "migrate-legacy"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="migrate old Memo scratch/archive recordings")
    command.add_argument("--dry-run", action="store_true", help="report without writing recordings")
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    summary = migrate_legacy(dry_run=args.dry_run)
    label = "would migrate" if args.dry_run else "migrated"
    print(f"{label}: {len(summary.migrated)}")
    print(f"skipped: {len(summary.skipped)}")
    print(f"failed: {len(summary.failed)}")
    for session_id in summary.migrated:
        print(f"{label}: {session_id}")
    for source, reason in summary.skipped:
        print(f"skipped: {source}: {reason}")
    for source, error in summary.failed:
        print(f"failed: {source}: {error}", file=sys.stderr)
    return 1 if summary.failed else 0
