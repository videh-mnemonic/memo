from __future__ import annotations

import signal
from types import SimpleNamespace

from memo.cli.commands import verify


def _args(**overrides: object) -> SimpleNamespace:
    defaults = {"session_id": None, "archive": False, "all_origins": False, "limit": None}
    return SimpleNamespace(**{**defaults, **overrides})


def test_archive_targets_this_machine_unless_asked_otherwise(monkeypatch) -> None:
    seen: list[object] = []

    def listing(*, origin: object = None) -> list[str]:
        seen.append(origin)
        return ["a", "b"]

    monkeypatch.setattr(verify, "list_archived_session_ids", listing)

    verify._targets(_args(archive=True))
    verify._targets(_args(archive=True, all_origins=True))

    # An archive is shared between machines. Checking one's own recordings is
    # the common case; reading back everyone else's is hours of downloading.
    assert seen[0] is not None
    assert seen[1] is None


def test_limit_caps_the_number_checked(monkeypatch, capsys) -> None:
    monkeypatch.setattr(verify, "list_archived_session_ids", lambda **_kw: ["a", "b", "c"])
    monkeypatch.setattr(
        verify, "verify_archived_session", lambda session_id, **_kw: {"steps": 1, "bytes": 2}
    )

    assert verify.run(_args(archive=True, limit=2)) == 0

    output = capsys.readouterr().out
    assert output.count("intact:") == 2
    assert "2 intact, 0 broken" in output


def test_limit_below_one_is_rejected(capsys) -> None:
    assert verify.run(_args(limit=0)) == 2
    assert "--limit must be at least 1" in capsys.readouterr().err


def test_terminate_unwinds_so_extraction_temporaries_are_removed(monkeypatch, capsys) -> None:
    monkeypatch.setattr(verify, "list_archived_session_ids", lambda **_kw: ["a", "b"])
    cleaned: list[str] = []

    def stubborn(session_id: str, **_kw: object) -> dict[str, int]:
        try:
            # Stand in for the temporary directory an extraction holds open;
            # dying on the default SIGTERM handler would strand it.
            signal.raise_signal(signal.SIGTERM)
            return {"steps": 1, "bytes": 2}
        finally:
            cleaned.append(session_id)

    monkeypatch.setattr(verify, "verify_archived_session", stubborn)

    assert verify.run(_args(archive=True)) == 130

    assert cleaned == ["a"]
    assert "interrupted after 0 of 2" in capsys.readouterr().err
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


def test_terminate_handler_is_restored_after_a_clean_run(monkeypatch) -> None:
    monkeypatch.setattr(verify, "list_archived_session_ids", lambda **_kw: ["a"])
    monkeypatch.setattr(
        verify, "verify_archived_session", lambda session_id, **_kw: {"steps": 1, "bytes": 2}
    )
    before = signal.getsignal(signal.SIGTERM)

    assert verify.run(_args(archive=True)) == 0

    assert signal.getsignal(signal.SIGTERM) is before
