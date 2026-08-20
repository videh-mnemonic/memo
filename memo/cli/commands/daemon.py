"""Inspect and stop the per-user Memo daemon."""

from __future__ import annotations

from typing import Any

from ...daemon.control import daemon_health, stop_daemon
from ...recording.paths import StoragePaths
from ...runtime import RUNTIME_ID

NAME = "daemon"


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="inspect or stop the Memo daemon")
    actions = command.add_subparsers(dest="daemon_action", required=True)
    status = actions.add_parser("status", help="show daemon identity and state")
    status.set_defaults(handler=run)
    stop = actions.add_parser("stop", help="stop the daemon when no terminals are attached")
    stop.add_argument(
        "--force",
        action="store_true",
        help="stop despite terminals recorded as attached",
    )
    stop.set_defaults(handler=run)


def run(args: Any) -> int:
    paths = StoragePaths.discover()
    if args.daemon_action == "status":
        health = daemon_health(paths)
        if health is None:
            print("daemon: stopped")
            return 0
        runtime_id = health.get("runtime_id")
        compatibility = "current" if runtime_id == RUNTIME_ID else "stale"
        print(f"daemon: running ({compatibility})")
        print(f"pid: {health.get('pid', 'unknown')}")
        print(f"version: {health.get('version', 'legacy')}")
        print(f"runtime: {runtime_id or 'legacy'}")
        return 0
    stopped, attachments = stop_daemon(paths, force=args.force)
    if attachments:
        roots = sorted({attachment.root for attachment in attachments})
        raise RuntimeError(
            f"refusing to stop daemon with {len(attachments)} attached terminal(s): "
            f"{', '.join(roots)}; close them or use memo daemon stop --force"
        )
    print("daemon: stopped" if stopped else "daemon: already stopped")
    return 0
