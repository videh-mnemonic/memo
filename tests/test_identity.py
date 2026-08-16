from memo import __version__
from memo.models import SessionOrigin


def test_version_and_live_origin(monkeypatch) -> None:
    monkeypatch.setattr("memo.recording.models.getpass.getuser", lambda: "alice")
    monkeypatch.setattr("memo.recording.models.socket.gethostname", lambda: "laptop")

    assert __version__ == "1.0.0"
    assert SessionOrigin.current() == SessionOrigin("1.0.0", "alice", "laptop")
