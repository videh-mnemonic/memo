"""Command-line interface for the standalone legacy migrator."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .migrate import migrate_legacy
from .s3_recompress import recompress_s3


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="memo-migrate-legacy",
        description="Migrate recordings created by the pre-daemon Memo prototype",
    )
    source = result.add_mutually_exclusive_group()
    source.add_argument(
        "--legacy-dir",
        type=Path,
        metavar="DIRECTORY",
        help="read unpacked legacy recording directories from DIRECTORY",
    )
    source.add_argument(
        "--recompress-s3",
        action="store_true",
        help="replace pre-Git S3 generations after exhaustive local and remote verification",
    )
    result.add_argument("--dry-run", action="store_true", help="report without writing recordings")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.recompress_s3:
            summary = recompress_s3(dry_run=args.dry_run)
        else:
            summary = migrate_legacy(legacy_dir=args.legacy_dir, dry_run=args.dry_run)
    except Exception as error:
        print(f"memo-migrate-legacy: {error}", file=sys.stderr)
        return 1
    if args.recompress_s3:
        label = "would recompress" if args.dry_run else "recompressed"
    else:
        label = "would migrate" if args.dry_run else "migrated"
    print(f"{label}: {len(summary.migrated)}")
    print(f"skipped: {len(summary.skipped)}")
    print(f"failed: {len(summary.failed)}")
    source_count = getattr(
        summary,
        "sources",
        len(summary.migrated) + len(summary.skipped) + len(summary.failed),
    )
    if source_count == 0:
        if args.recompress_s3:
            print("memo-migrate-legacy: no indexed S3 recordings found", file=sys.stderr)
        else:
            print(
                "memo-migrate-legacy: no legacy recordings found; pass an old Memo home "
                "or a directory containing recording folders",
                file=sys.stderr,
            )
    for session_id in summary.migrated:
        print(f"{label}: {session_id}")
    for source, reason in summary.skipped:
        print(f"skipped: {source}: {reason}")
    for source, error in summary.failed:
        print(f"failed: {source}: {error}", file=sys.stderr)
    if args.recompress_s3 and summary.migrated:
        saved = summary.original_bytes - summary.replacement_bytes
        print(f"bytes: {summary.original_bytes} -> {summary.replacement_bytes} ({saved} saved)")
    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
