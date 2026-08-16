"""Import historical native agent sessions into standalone Memo recordings."""

from __future__ import annotations

import sys
from typing import Any

from ...agents.session_import import import_native_sessions

NAME = "import"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="recover native Claude and Codex sessions")
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    del args
    summary = import_native_sessions()
    print(f"imported: {len(summary.imported)}")
    print(f"refreshed: {len(summary.refreshed)}")
    print(f"already captured: {len(summary.skipped)}")
    print(f"unimportable: {len(summary.failed)}")
    for source, error in summary.failed:
        print(f"unimportable: {source}: {error}", file=sys.stderr)
    return 1 if summary.failed else 0
