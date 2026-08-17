import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from memo.cli.commands.status import _age, _format_size, _session_size, render_status
from memo.daemon.registry import Registry
from memo.daemon.server import TERMINAL_STALE_SECONDS
from memo.recording.filesystem import atomic_write
from memo.recording.metadata import DirectorySession, SessionOrigin
from memo.recording.paths import StoragePaths
from memo.recording.snapshots import StepPublisher
from memo.recording.store import SessionStore


def _paths(tmp_path: Path) -> StoragePaths:
    return StoragePaths(tmp_path)


def test_status_reports_operational_session_summary(tmp_path: Path, monkeypatch) -> None:
    now = datetime(2026, 8, 15, 20, 0, tzinfo=UTC)
    root = tmp_path / "root"
    root.mkdir()
    paths = _paths(tmp_path / "memo-home")
    store = SessionStore(paths)
    session = DirectorySession(
        "abc123",
        str(root.resolve()),
        (now - timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
        now.isoformat().replace("+00:00", "Z"),
        SessionOrigin("1.0.0", "user", "host"),
        "active",
    )
    store.create(session)
    publisher = StepPublisher(store)
    for step in range(15):
        (root / "value.txt").write_text(str(step))
        publisher.publish(session)
    head_path = store.session_path(session.session_id) / "steps/14.json"
    head = head_path.read_text().replace(
        SessionStore(paths).head(session.session_id).created_utc,
        (now - timedelta(seconds=18)).isoformat().replace("+00:00", "Z"),
    )
    atomic_write(head_path, head.encode())
    session.last_pushed_step = 12
    session.last_pushed_digest = "0" * 64
    session.remote_object = "generation"
    store.update_session(session)
    with Registry(paths.registry) as registry:
        registry.create(root, session.created_utc, session.session_id)
        registry.allocate_attachment(session.session_id, session.created_utc, "terminal")
    monkeypatch.setattr("memo.cli.commands.status._session_size", lambda _path: 284 * 1024 * 1024)

    lines = render_status(paths, now=now).splitlines()

    assert lines[0].split() == [
        "SESSION",
        "ROOT",
        "STATE",
        "SCOPE",
        "AGE",
        "LAST",
        "TERMINALS",
        "STEPS",
        "SIZE",
        "ARCHIVED",
    ]
    assert lines[1].split() == [
        "abc123",
        str(root.resolve()),
        "active",
        "partial",
        "2h",
        "18s",
        "ago",
        "1",
        "15",
        "284",
        "MiB",
        "13/15",
    ]


def test_status_uses_count_based_steps_and_never_archived_marker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = datetime(2026, 8, 15, 20, 0, tzinfo=UTC)
    root = tmp_path / "root"
    root.mkdir()
    paths = _paths(tmp_path / "memo-home")
    store = SessionStore(paths)
    session = DirectorySession(
        "session",
        str(root.resolve()),
        now.isoformat(),
        now.isoformat(),
        SessionOrigin("1.0.0", "user", "host"),
        "complete",
    )
    store.create(session)
    StepPublisher(store).publish(session)
    monkeypatch.setattr("memo.cli.commands.status._session_size", lambda _path: 0)

    line = render_status(paths, now=now).splitlines()[1]

    assert line.split()[-4:] == ["1", "0", "B", "—"]
    assert "complete" in line


def test_relative_times_are_compact() -> None:
    now = datetime(2026, 8, 15, 20, 0, tzinfo=UTC)

    def value(delta: timedelta) -> str:
        return (now - delta).isoformat().replace("+00:00", "Z")

    assert _age(value(timedelta(seconds=2)), now, ago=True) == "just now"
    assert _age(value(timedelta(seconds=18)), now, ago=True) == "18s ago"
    assert _age(value(timedelta(minutes=8)), now) == "8m"
    assert _age(value(timedelta(days=3)), now) == "3d"
    assert _age("not-a-timestamp", now) == "—"


def test_sizes_use_binary_units() -> None:
    assert _format_size(0) == "0 B"
    assert _format_size(1024) == "1 KiB"
    assert _format_size(91 * 1024 * 1024) == "91 MiB"
    assert _format_size(int(1.3 * 1024**3)) == "1.3 GiB"


def test_session_size_counts_regular_files_recursively(tmp_path: Path) -> None:
    (tmp_path / "nested").mkdir()
    (tmp_path / "one").write_bytes(b"123")
    (tmp_path / "nested/two").write_bytes(b"4567")
    (tmp_path / "link").symlink_to(tmp_path / "one")

    assert _session_size(tmp_path) == 7


def test_status_reports_no_sessions_when_storage_is_empty(tmp_path: Path) -> None:
    assert render_status(_paths(tmp_path)) == "No sessions.\n"


def test_status_can_select_one_exact_session(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path / "memo-home")
    store = SessionStore(paths)
    for session_id in ("one", "two"):
        root = tmp_path / session_id
        root.mkdir()
        store.create(
            DirectorySession(
                session_id,
                str(root.resolve()),
                "now",
                "now",
                SessionOrigin("1.0.0", "user", "host"),
                "complete",
            )
        )
    monkeypatch.setattr("memo.cli.commands.status._session_size", lambda _path: 0)

    output = render_status(paths, session_id="two")

    assert "Session: two" in output
    assert "State: complete" in output
    assert "Lifecycle: complete" in output
    assert "Session: one" not in output


def test_status_can_filter_active_recordings(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path / "memo-home")
    store = SessionStore(paths)
    for session_id, state in (("active", "active"), ("complete", "complete")):
        root = tmp_path / session_id
        root.mkdir()
        store.create(
            DirectorySession(
                session_id,
                str(root.resolve()),
                "now",
                "now",
                SessionOrigin("1.0.0", "user", "host"),
                state,
            )
        )
    with Registry(paths.registry) as registry:
        registry.create(tmp_path / "active", "now", "active")
    monkeypatch.setattr("memo.cli.commands.status._session_size", lambda _path: 0)

    output = render_status(paths, active_only=True)

    assert "active" in output
    assert "complete" not in output


def test_status_marks_stale_terminals_inactive(tmp_path: Path, monkeypatch) -> None:
    now = datetime(2026, 8, 15, 20, 0, tzinfo=UTC)
    paths = _paths(tmp_path / "memo-home")
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(paths)
    session = DirectorySession(
        "session",
        str(root.resolve()),
        now.isoformat(),
        now.isoformat(),
        SessionOrigin("1.0.0", "user", "host"),
        "active",
    )
    store.create(session)
    StepPublisher(store).publish(session)
    with Registry(paths.registry) as registry:
        registry.create(root, session.created_utc, session.session_id)
        attachment = registry.allocate_attachment(
            session.session_id, session.created_utc, "terminal"
        )
        registry.touch_attachment(
            attachment.terminal_id,
            time.time_ns() - int((TERMINAL_STALE_SECONDS + 1) * 1_000_000_000),
        )
    monkeypatch.setattr("memo.cli.commands.status._session_size", lambda _path: 0)

    summary = render_status(paths, now=now).splitlines()[1]
    detail = render_status(paths, now=now, session_id="session")

    assert "  0  " in summary
    assert "terminal: stale" in detail


def test_single_status_rejects_list_only_options(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="single-session status"):
        render_status(_paths(tmp_path), session_id="session", limit=1)

    with pytest.raises(ValueError, match="--active"):
        render_status(_paths(tmp_path), session_id="session", active_only=True)


def test_status_appends_remote_only_sessions_and_applies_combined_limit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path / "memo-home")
    store = SessionStore(paths)
    for session_id in ("local-a", "local-b"):
        root = tmp_path / session_id
        root.mkdir()
        session = DirectorySession(
            session_id,
            str(root.resolve()),
            "now",
            "now",
            SessionOrigin("1.0.0", "user", "host"),
            "complete",
        )
        store.create(session)
    monkeypatch.setattr("memo.cli.commands.status._session_size", lambda _path: 0)
    monkeypatch.setattr(
        "memo.transport.list_archived_session_ids",
        lambda: ["local-a", "remote-a", "remote-b"],
    )

    lines = render_status(paths, include_archive=True, limit=3).splitlines()

    assert [line.split()[0] for line in lines[1:]] == ["local-a", "local-b", "remote-a"]
    assert "archived" in lines[-1]


def test_status_limit_can_be_satisfied_without_listing_archive(
    tmp_path: Path,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path / "memo-home")
    root = tmp_path / "root"
    root.mkdir()
    SessionStore(paths).create(
        DirectorySession(
            "local",
            str(root.resolve()),
            "now",
            "now",
            SessionOrigin("1.0.0", "user", "host"),
            "complete",
        )
    )
    monkeypatch.setattr("memo.cli.commands.status._session_size", lambda _path: 0)
    monkeypatch.setattr(
        "memo.transport.list_archived_session_ids",
        lambda: (_ for _ in ()).throw(AssertionError("archive should not be listed")),
    )

    assert "local" in render_status(paths, include_archive=True, limit=1)
