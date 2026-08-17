"""Command-line interface for the standalone legacy migrator."""

from __future__ import annotations

import argparse
import sys

from .migrate import migrate_legacy


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="memo-migrate-legacy",
        description="Migrate recordings created by the pre-daemon Memo prototype",
    )
    result.add_argument("--dry-run", action="store_true", help="report without writing recordings")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        summary = migrate_legacy(dry_run=args.dry_run)
    except Exception as error:
        print(f"memo-migrate-legacy: {error}", file=sys.stderr)
        return 1
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


if __name__ == "__main__":
    raise SystemExit(main())
