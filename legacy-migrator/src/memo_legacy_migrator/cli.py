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


def _worker_count(value: str) -> int:
    workers = int(value)
    if not 1 <= workers <= 8:
        raise argparse.ArgumentTypeError("must be between 1 and 8")
    return workers


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
    result.add_argument(
        "--workers",
        type=_worker_count,
        metavar="N",
        help="process N S3 sessions concurrently (1-8; default: 4)",
    )
    result.add_argument(
        "--session",
        dest="session_ids",
        action="append",
        metavar="SESSION_ID",
        help="upgrade only SESSION_ID; may be repeated and requires --upgrade-s3",
    )
    result.add_argument(
        "--best-effort",
        action="store_true",
        help=(
            "substitute missing filesystem states with the nearest verified state, "
            "embed a repair report, and retain the original S3 archive"
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
    if args.workers is not None and not args.upgrade_s3:
        print("memo-migrate-legacy: --workers requires --upgrade-s3", file=sys.stderr)
        return 2
    if args.session_ids is not None and not args.upgrade_s3:
        print("memo-migrate-legacy: --session requires --upgrade-s3", file=sys.stderr)
        return 2
    if args.best_effort and not args.upgrade_s3:
        print("memo-migrate-legacy: --best-effort requires --upgrade-s3", file=sys.stderr)
        return 2
    previous_handler: signal.Handlers | None = None
    try:
        if args.upgrade_s3:
            previous_handler = _stop_on_terminate()
            with ProgressPair(show_eta=True) as bars:
                summary = upgrade_s3(
                    dry_run=args.dry_run,
                    scratch_dir=args.scratch_dir,
                    workers=args.workers or 4,
                    session_ids=args.session_ids,
                    best_effort=args.best_effort,
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
    for session_id, substitutions in getattr(summary, "best_effort", {}).items():
        action = "would retain" if args.dry_run else "retained"
        print(
            f"BEST EFFORT: {session_id}: substituted {substitutions} filesystem step(s); "
            f"{action} original S3 archive",
            file=sys.stderr,
        )
    if args.upgrade_s3 and summary.migrated:
        retained = getattr(summary, "retained_original_bytes", 0)
        effective_after = summary.replacement_bytes + retained
        difference = summary.original_bytes - effective_after
        if retained:
            change = f"{difference} saved" if difference >= 0 else f"{-difference} added"
            print(
                f"bytes: {summary.original_bytes} -> {effective_after} "
                f"({change}; best-effort originals retained)"
            )
        else:
            print(
                f"bytes: {summary.original_bytes} -> {summary.replacement_bytes} "
                f"({difference} saved)"
            )
    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
