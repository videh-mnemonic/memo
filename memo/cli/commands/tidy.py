"""Import sessions, push recordings, and remove safely archived local copies."""

from __future__ import annotations

import sys
from typing import Any

from ...agents.session_import import import_native_sessions
from ...daemon.client import push, remove_archived
from ..progress import ProgressBar

NAME = "tidy"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(
        NAME, help="import, push, and remove safely archived recordings"
    )
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    del args
    with ProgressBar() as progress:
        kwargs = {"progress": progress.update} if progress.enabled else {}
        imported = import_native_sessions(**kwargs)
        progress.update(1, 3, "pushing recordings")
        pushed = push()
        progress.update(2, 3, "removing archived local copies")
        removed = remove_archived([session_id for session_id, _ in pushed["failed"]])
        progress.update(3, 3, "tidy complete")
    print(f"imported: {len(imported.imported)}")
    print(f"refreshed: {len(imported.refreshed)}")
    print(f"already captured: {len(imported.skipped)}")
    print(f"unimportable: {len(imported.failed)}")
    for source, error in imported.failed:
        print(f"unimportable: {source}: {error}", file=sys.stderr)

    for session_id in pushed["pushed"]:
        print(f"pushed: {session_id}")
    for session_id in pushed["skipped"]:
        print(f"skipped: unchanged: {session_id}")
    for session_id, error in pushed["failed"]:
        print(f"failed: {session_id}: {error}", file=sys.stderr)

    for session_id in removed["removed"]:
        print(f"removed: {session_id}")
    for session_id, reason in removed["retained"]:
        print(f"retained: {session_id}: {reason}")
    for session_id, error in removed["failed"]:
        print(f"failed to remove: {session_id}: {error}", file=sys.stderr)
    return 1 if imported.failed or pushed["failed"] or removed["failed"] else 0
