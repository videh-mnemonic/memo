"""Command-line interface for the standalone legacy migrator."""

from __future__ import annotations

import argparse
import signal
import sys
from pathlib import Path
from types import FrameType

from memo.cli.progress import ProgressPair

from .migrate import migrate_legacy
from .s3_recompress import upgrade_s3


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
        "--upgrade-s3",
        "--recompress-s3",
        dest="upgrade_s3",
        action="store_true",
        help="upgrade historical S3 session and transport formats after exhaustive verification",
    )
    result.add_argument("--dry-run", action="store_true", help="report without writing recordings")
    result.add_argument(
        "--scratch-dir",
        type=Path,
        metavar="DIRECTORY",
        help=(
            "place disposable S3 upgrade data under DIRECTORY; defaults to the user cache directory"
        ),
    )
    return result


def _stop_on_terminate() -> signal.Handlers:
    def terminate(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    return signal.signal(signal.SIGTERM, terminate)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.scratch_dir is not None and not args.upgrade_s3:
        print("memo-migrate-legacy: --scratch-dir requires --upgrade-s3", file=sys.stderr)
        return 2
    previous_handler: signal.Handlers | None = None
    try:
        if args.upgrade_s3:
            previous_handler = _stop_on_terminate()
            with ProgressPair(show_eta=True) as bars:
                summary = upgrade_s3(
                    dry_run=args.dry_run,
                    scratch_dir=args.scratch_dir,
                    progress=bars.update_overall if bars.enabled else None,
                    item_progress=bars.update_current if bars.enabled else None,
                )
        else:
            summary = migrate_legacy(legacy_dir=args.legacy_dir, dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("memo-migrate-legacy: interrupted; local scratch data removed", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"memo-migrate-legacy: {error}", file=sys.stderr)
        return 1
    finally:
        if previous_handler is not None:
            signal.signal(signal.SIGTERM, previous_handler)
    if args.upgrade_s3:
        label = "would upgrade" if args.dry_run else "upgraded"
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
        if args.upgrade_s3:
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
    if args.upgrade_s3 and summary.migrated:
        saved = summary.original_bytes - summary.replacement_bytes
        print(f"bytes: {summary.original_bytes} -> {summary.replacement_bytes} ({saved} saved)")
    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
