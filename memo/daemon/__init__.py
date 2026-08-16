"""Memo daemon server, client, protocol, and runtime registry."""

from typing import Any

from .client import attach, end, ensure_daemon, push, remove_archived
from ..config import TransportConfig

__all__ = [
    "MemoDaemon",
    "main",
    "attach",
    "end",
    "ensure_daemon",
    "push",
    "remove_archived",
    "TransportConfig",
]


def __getattr__(name: str) -> Any:
    if name in {"MemoDaemon", "main"}:
        from .server import MemoDaemon, main

        return {"MemoDaemon": MemoDaemon, "main": main}[name]
    raise AttributeError(name)
