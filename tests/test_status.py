from pathlib import Path

from memo.config import Paths
from memo.models import DirectorySession
from memo.session_store import SessionStore
from memo.status import render_status
from memo.step import StepPublisher


def _paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path)


def test_status_lists_directory_step(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    paths = _paths(tmp_path)
    store = SessionStore(paths)
    session = DirectorySession("session", str(root.resolve()), "namespace", "now", "now", "complete")
    store.create(session)
    StepPublisher(store).publish(session)
    output = render_status(paths)
    assert "STEP" in output
    assert "complete" in output
    assert "session" in output
    assert "namespace" in output
    assert "  0" in output


def test_status_reports_no_sessions_when_storage_is_empty(tmp_path: Path) -> None:
    assert render_status(_paths(tmp_path)) == "No sessions.\n"
