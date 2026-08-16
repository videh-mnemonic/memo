from memo import __version__
from memo.recording.metadata import SessionOrigin


def test_version_and_live_origin(monkeypatch) -> None:
    monkeypatch.setattr("memo.recording.metadata.getpass.getuser", lambda: "alice")
    monkeypatch.setattr("memo.recording.metadata.socket.gethostname", lambda: "laptop")

    assert __version__ == "1.0.0"
    assert SessionOrigin.current() == SessionOrigin("1.0.0", "alice", "laptop")
