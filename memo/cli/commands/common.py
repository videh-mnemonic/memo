"""Provide small helpers shared by multiple CLI commands."""

from ...transport import ensure_local_session


def require_local_session(session_id: str) -> None:
    ensure_local_session(session_id)
