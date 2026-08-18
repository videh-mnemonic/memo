from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from memo_legacy_migrator.cli import main
from memo_legacy_migrator.migrate import LegacySource, legacy_sources, migrate_legacy

from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore


def _git(args: list[str], cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


def test_migrate_legacy_scratch_recording(tmp_path: Path) -> None:
    source_repo = tmp_path / "repo"
    source_repo.mkdir()
    _git(["init"], source_repo)
    _git(["config", "user.email", "test@example.com"], source_repo)
    _git(["config", "user.name", "Test"], source_repo)
    (source_repo / "note.txt").write_text("legacy\n")
    _git(["add", "."], source_repo)
    _git(["commit", "-m", "initial"], source_repo)

    paths = StoragePaths(tmp_path / "home")
    legacy = paths.home / "scratch" / "legacy-session"
    (legacy / "git").mkdir(parents=True)
    (legacy / "legs" / "001").mkdir(parents=True)
    (legacy / "traces").mkdir()
    _git(["bundle", "create", str(legacy / "git" / "initial.bundle"), "HEAD"], source_repo)
    (legacy / "traces" / "leg-001.jsonl").write_text(
        json.dumps({"session_id": "legacy-session", "type": "user", "content": "old prompt"})
        + "\n"
    )
    (legacy / "meta.json").write_text(
        json.dumps(
            {
                "session_id": "legacy-session",
                "tool": "codex",
                "repo_kind": "real",
                "repo_root": str(source_repo),
                "repo_name": "repo",
                "remote": "",
                "canonical_remote": "",
                "archive_namespace": "local_repo",
                "initial_head": "unused",
                "final_head": "unused",
                "first_seen_utc": "2026-01-01T00:00:00Z",
                "last_activity_utc": "2026-01-01T00:01:00Z",
                "coverage": "full",
                "legs": [
                    {
                        "leg_id": "001",
                        "tool_args": ["resume", "legacy-session"],
                        "start_utc": "2026-01-01T00:00:00Z",
                        "end_utc": "2026-01-01T00:01:00Z",
                        "exit_code": 0,
                        "trace_file": "leg-001.jsonl",
                        "complete": True,
                    }
                ],
            }
        )
        + "\n"
    )

    summary = migrate_legacy(paths)

    assert summary.migrated == ["legacy-session"]
    assert summary.failed == []
    store = SessionStore(paths)
    session = store.load_session("legacy-session")
    assert session.state == "complete"
    assert session.capture_scope == "full"
    head = store.head("legacy-session")
    assert head is not None
    assert head.agent_runs == ["legacy-001"]
    snapshot = store.session_path("legacy-session") / head.snapshot
    assert (snapshot / "note.txt").read_text() == "legacy\n"
    assert not (snapshot / ".git").exists()


def test_cli_dry_run_reports_without_writing(monkeypatch, capsys) -> None:
    requested_directory = Path("/legacy-recordings")
    received: dict[str, object] = {}

    def fake_migrate_legacy(*, legacy_dir: Path | None, dry_run: bool) -> SimpleNamespace:
        received.update(legacy_dir=legacy_dir, dry_run=dry_run)
        return SimpleNamespace(migrated=["legacy-session"], skipped=[], failed=[])

    monkeypatch.setattr(
        "memo_legacy_migrator.cli.migrate_legacy",
        fake_migrate_legacy,
    )

    assert main(["--legacy-dir", str(requested_directory), "--dry-run"]) == 0
    assert received == {"legacy_dir": requested_directory, "dry_run": True}
    assert capsys.readouterr().out == (
        "would migrate: 1\n"
        "skipped: 0\n"
        "failed: 0\n"
        "would migrate: legacy-session\n"
    )


def test_legacy_sources_can_use_explicit_directory(tmp_path: Path) -> None:
    paths = StoragePaths(tmp_path / "current-home")
    legacy_dir = tmp_path / "old-recordings"
    recording = legacy_dir / "legacy-session"
    recording.mkdir(parents=True)
    (recording / "meta.json").write_text("{}\n")
    (legacy_dir / "not-a-recording").mkdir()

    assert legacy_sources(paths, legacy_dir=legacy_dir) == [
        LegacySource("legacy-dir:legacy-session", recording)
    ]


def test_legacy_sources_can_use_explicit_old_memo_home(tmp_path: Path) -> None:
    paths = StoragePaths(tmp_path / "current-home")
    legacy_home = tmp_path / "old-home"
    recording = legacy_home / "scratch" / "legacy-session"
    recording.mkdir(parents=True)
    (recording / "meta.json").write_text("{}\n")
    archive = legacy_home / "archive" / "namespace" / "legacy-session.tar.gz"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"not really a tarball")

    assert legacy_sources(paths, legacy_dir=legacy_home) == [
        LegacySource("legacy-dir:scratch:legacy-session", recording),
        LegacySource("legacy-dir:archive:namespace/legacy-session.tar.gz", archive, True),
    ]


def test_cli_reports_when_no_sources_are_found(monkeypatch, capsys) -> None:
    def fake_migrate_legacy(*, legacy_dir: Path | None, dry_run: bool) -> SimpleNamespace:
        return SimpleNamespace(sources=0, migrated=[], skipped=[], failed=[])

    monkeypatch.setattr(
        "memo_legacy_migrator.cli.migrate_legacy",
        fake_migrate_legacy,
    )

    assert main(["--dry-run"]) == 0
    captured = capsys.readouterr()
    assert captured.out == "would migrate: 0\nskipped: 0\nfailed: 0\n"
    assert "no legacy recordings found" in captured.err
