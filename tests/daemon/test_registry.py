from __future__ import annotations

from pathlib import Path

import pytest

from memo.daemon.registry import (
    AgentLaunch,
    OverlappingRootError,
    Registry,
    SandboxShellLaunch,
)


def test_canonical_path_create_persists_and_rejects_duplicate(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    database = tmp_path / "registry.sqlite"
    with Registry(database) as registry:
        first = registry.create(root, "now", "session-one")
        with pytest.raises(RuntimeError, match="already exists"):
            registry.create(root / ".", "later")
    with Registry(database) as registry:
        assert registry.lookup(root) == first


def test_sibling_roots_are_allowed(tmp_path: Path) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    with Registry(tmp_path / "registry.sqlite") as registry:
        registry.create(left, "now")
        registry.create(right, "now")
        assert len(registry.list_active()) == 2


def test_overlapping_roots_are_rejected_without_partial_insert(tmp_path: Path) -> None:
    root = tmp_path / "work"
    child = root / "child"
    child.mkdir(parents=True)
    with Registry(tmp_path / "registry.sqlite") as registry:
        registry.create(root, "now")
        with pytest.raises(OverlappingRootError, match="overlaps"):
            registry.create(child, "now")
        assert len(registry.list_active()) == 1


def test_stale_entries_are_removed(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    with Registry(tmp_path / "registry.sqlite") as registry:
        active = registry.create(root, "now", "stale")
        assert registry.remove_stale(tmp_path / "archive") == [active.session_id]
        assert registry.list_active() == []


def test_sandbox_metadata_and_shell_launches_round_trip(tmp_path: Path) -> None:
    root = tmp_path / "work"
    root.mkdir()
    with Registry(tmp_path / "registry.sqlite") as registry:
        registry.create(root, "now", "session")
        registry.allocate_attachment("session", "now", "terminal")
        registry.add_launch(
            AgentLaunch(
                "agent",
                "session",
                "terminal",
                "codex",
                str(root),
                ["codex"],
                "now",
                effective_command=["codex", "--dangerously-bypass-approvals-and-sandbox"],
                sandbox_mode="sandbox",
                policy_summary={"root": str(root)},
                policy_digest="a" * 64,
                guidance_digest="b" * 64,
            )
        )
        agent = registry.launch("agent")
        assert agent is not None
        assert agent.sandbox_mode == "sandbox"
        assert agent.policy_summary == {"root": str(root)}

        registry.add_sandbox_shell_launch(
            SandboxShellLaunch(
                "shell",
                "session",
                "terminal",
                str(root),
                ["/bin/sh"],
                "now",
                {"root": str(root)},
                "c" * 64,
            )
        )
        completed = registry.finish_sandbox_shell_launch("shell", "later", 7)
        assert completed.exit_code == 7
        assert registry.sandbox_shell_launches("session") == [completed]


def test_registry_migrates_existing_agent_launch_table(tmp_path: Path) -> None:
    import sqlite3

    database = tmp_path / "registry.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        "CREATE TABLE agent_launches ("
        "launch_id TEXT PRIMARY KEY, session_id TEXT, terminal_id TEXT, harness TEXT, "
        "cwd TEXT, command_json TEXT, started_utc TEXT, ended_utc TEXT, exit_code INTEGER)"
    )
    connection.commit()
    connection.close()

    with Registry(database) as registry:
        columns = {
            row[1] for row in registry.connection.execute("PRAGMA table_info(agent_launches)")
        }
    assert {"sandbox_mode", "policy_digest", "guidance_digest"} <= columns
