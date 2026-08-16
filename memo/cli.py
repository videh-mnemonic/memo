from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from .daemon import end, push, remove_archived
from .load import replay_session, terminal_ids, trace_json, write_traces
from .relay import run as run_relay
from .status import render_status


COMMANDS = {"end", "status", "traces", "replay", "push", "pull", "import", "tidy"}


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def _ensure_local_session(session_id: str) -> None:
    from .transport import ensure_local_session

    ensure_local_session(session_id)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="memo", description="Record and replay directory sessions")
    subparsers = result.add_subparsers(dest="command")
    finish = subparsers.add_parser("end", help="finish a recording")
    finish.add_argument("path", nargs="?", type=Path)
    finish.add_argument("--scope", choices=("partial", "full"))
    subparsers.add_parser("import", help="recover native Claude and Codex sessions")
    subparsers.add_parser(
        "tidy", help="import, push, and remove safely archived recordings"
    )
    status = subparsers.add_parser("status", help="list recordings")
    status.add_argument("session_id", nargs="?", help="show one recording")
    status.add_argument("--include-archive", action="store_true",
                        help="include remote-only archived recordings")
    status.add_argument("--limit", type=_positive_int,
                        help="maximum number of recordings to display")
    traces = subparsers.add_parser("traces", help="export agent or terminal traces")
    traces.add_argument("session_id")
    traces.add_argument("--path", type=Path)
    traces.add_argument("--terminals")
    traces.add_argument("--list-terminals", action="store_true",
                        help="list terminal stream IDs")
    traces.add_argument("--raw", action="store_true", help="export native agent records")
    replay = subparsers.add_parser("replay", help="restore a recorded step")
    replay.add_argument("session_id")
    replay.add_argument("at")
    replay.add_argument("directory", type=Path)
    replay.add_argument("--include-prompts", action="store_true")
    replay.add_argument("--force", action="store_true")
    upload = subparsers.add_parser("push", help="push recordings")
    upload.add_argument("session_id", nargs="?")
    download = subparsers.add_parser("pull", help="pull a recording")
    download.add_argument("session_id")
    download.add_argument("--force", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        try:
            return run_relay(Path.cwd())
        except Exception as error:
            print(f"memo: {error}", file=sys.stderr)
            return 1
    elif argv[0] not in COMMANDS and (argv[0] == "." or Path(argv[0]).exists()):
        try:
            return run_relay(Path(argv[0]))
        except Exception as error:
            print(f"memo: {error}", file=sys.stderr)
            return 1
    args = parser().parse_args(argv)
    try:
        if args.command == "status":
            if args.session_id is not None:
                if args.include_archive or args.limit is not None:
                    raise ValueError(
                        "single-session status cannot use --include-archive or --limit"
                    )
                _ensure_local_session(args.session_id)
            print(render_status(
                include_archive=args.include_archive,
                limit=args.limit,
                session_id=args.session_id,
            ), end="")
        elif args.command == "end":
            target_path = args.path
            environment_session = os.environ.get("MEMO_SESSION_ID") if target_path is None else None
            environment_terminal = os.environ.get("MEMO_TERMINAL_ID")
            prompt_scope = args.scope is None and sys.stdin.isatty()
            response = end(
                target_path if target_path is not None or environment_session else Path.cwd(),
                session_id=environment_session,
                terminal_id=environment_terminal,
                capture_scope=args.scope,
                prompt_scope=prompt_scope,
            )
            confirmed = False
            expected_revision = None
            while (response.get("confirmation_required") or response.get("stale")
                   or response.get("scope_confirmation_required")):
                if response.get("scope_confirmation_required"):
                    answer = input("Did Memo capture all intended work for this session? [y/N] ")
                    selected_scope = "full" if answer.strip().lower() in {"y", "yes"} else "partial"
                    response = end(
                        target_path if target_path is not None or environment_session else Path.cwd(),
                        session_id=environment_session,
                        terminal_id=environment_terminal,
                        confirmed=confirmed,
                        expected_revision=expected_revision,
                        capture_scope=selected_scope,
                    )
                    continue
                count = int(response["other_terminals"])
                if response.get("stale"):
                    print("The recording's attached terminals changed; confirmation is required again.")
                answer = input(
                    f"This recording has {count} other attached terminal"
                    f"{'s' if count != 1 else ''}.\n"
                    "End the recording for all terminals? [y/N] "
                )
                if answer.strip().lower() not in {"y", "yes"}:
                    print("recording unchanged")
                    return 0
                confirmed = True
                expected_revision = int(response["revision"])
                response = end(
                    target_path if target_path is not None or environment_session else Path.cwd(),
                    session_id=environment_session,
                    terminal_id=environment_terminal,
                    confirmed=confirmed,
                    expected_revision=expected_revision,
                    capture_scope=args.scope,
                    prompt_scope=prompt_scope,
                )
            action = "already complete" if response["already_complete"] else "completed"
            print(f"{action}: {response['session_id']} step={response['step']}")
            if response.get("cloud") == "pending":
                print("cloud upload started; automatic retry remains enabled", file=sys.stderr)
        elif args.command == "import":
            from .agents.importer import import_native_sessions

            summary = import_native_sessions()
            print(f"imported: {len(summary.imported)}")
            print(f"refreshed: {len(summary.refreshed)}")
            print(f"already captured: {len(summary.skipped)}")
            print(f"unimportable: {len(summary.failed)}")
            for source, error in summary.failed:
                print(f"unimportable: {source}: {error}", file=sys.stderr)
            return 1 if summary.failed else 0
        elif args.command == "tidy":
            from .agents.importer import import_native_sessions

            imported = import_native_sessions()
            print(f"imported: {len(imported.imported)}")
            print(f"refreshed: {len(imported.refreshed)}")
            print(f"already captured: {len(imported.skipped)}")
            print(f"unimportable: {len(imported.failed)}")
            for source, error in imported.failed:
                print(f"unimportable: {source}: {error}", file=sys.stderr)

            pushed = push()
            for session_id in pushed["pushed"]:
                print(f"pushed: {session_id}")
            for session_id in pushed["skipped"]:
                print(f"skipped: unchanged: {session_id}")
            for session_id, error in pushed["failed"]:
                print(f"failed: {session_id}: {error}", file=sys.stderr)

            removed = remove_archived([session_id for session_id, _ in pushed["failed"]])
            for session_id in removed["removed"]:
                print(f"removed: {session_id}")
            for session_id, reason in removed["retained"]:
                print(f"retained: {session_id}: {reason}")
            for session_id, error in removed["failed"]:
                print(f"failed to remove: {session_id}: {error}", file=sys.stderr)
            return 1 if imported.failed or pushed["failed"] or removed["failed"] else 0
        elif args.command == "push":
            response = push(args.session_id)
            for session_id in response["pushed"]:
                print(f"pushed: {session_id}")
            for session_id in response["skipped"]:
                print(f"skipped: unchanged: {session_id}")
            for session_id, error in response["failed"]:
                print(f"failed: {session_id}: {error}", file=sys.stderr)
            return 1 if response["failed"] else 0
        elif args.command == "pull":
            from .transport import pull_session
            destination = pull_session(args.session_id, force=args.force)
            print(f"pulled: {args.session_id} path={destination}")
        elif args.command == "traces":
            _ensure_local_session(args.session_id)
            if args.list_terminals:
                if args.terminals is not None or args.path is not None or args.raw:
                    raise ValueError(
                        "--list-terminals cannot be combined with --terminals, --path, or --raw"
                    )
                values = terminal_ids(args.session_id)
                print("\n".join(values) if values else "No terminal streams.")
                return 0
            selected_terminal_ids = None
            if args.terminals is not None:
                selected_terminal_ids = [
                    value.strip() for value in args.terminals.split(",")
                ]
                if (not selected_terminal_ids
                        or any(not value for value in selected_terminal_ids)):
                    raise ValueError("terminal IDs must be a comma-separated nonempty list")
            if args.path is None or str(args.path) == "-":
                print(trace_json(
                    args.session_id, selected_terminal_ids, raw=args.raw
                ), end="")
            else:
                write_traces(
                    args.session_id, args.path, selected_terminal_ids, raw=args.raw
                )
        elif args.command == "replay":
            _ensure_local_session(args.session_id)
            destination = replay_session(
                args.session_id, args.at, args.directory, args.include_prompts, args.force
            )
            print(f"replayed: {args.session_id} step={args.at} path={destination}")
        else:
            parser().error("a command is required")
        return 0
    except Exception as error:
        print(f"memo: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
