"""Build the Memo CLI parser and dispatch parsed commands."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

from ..recording.relay import run as run_relay
from ..transport.config import S3Config
from .commands import (
    end,
    import_sessions,
    pull,
    push,
    replay,
    sandbox,
    status,
    tidy,
    traces,
    verify,
)

COMMAND_MODULES = (
    end,
    import_sessions,
    tidy,
    status,
    traces,
    replay,
    push,
    pull,
    verify,
    sandbox,
)
COMMANDS = {module.NAME for module in COMMAND_MODULES}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="memo", description="Record and replay directory sessions"
    )
    subparsers = result.add_subparsers(dest="command")
    for module in COMMAND_MODULES:
        module.configure(subparsers)
    return result


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        return _run_with_s3(lambda: _run_relay(Path.cwd()))
    if argv[0] not in COMMANDS and (argv[0] == "." or Path(argv[0]).exists()):
        return _run_with_s3(lambda: _run_relay(Path(argv[0])))
    command_parser = parser()
    args = command_parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        command_parser.error("a command is required")
    return _run_with_s3(lambda: handler(args))


def _run_with_s3(operation: Callable[[], int]) -> int:
    try:
        S3Config.discover(required=True)
        return operation()
    except Exception as error:
        print(f"memo: {error}", file=sys.stderr)
        return 1


def _run_relay(path: Path) -> int:
    try:
        return run_relay(path)
    except Exception as error:
        print(f"memo: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
