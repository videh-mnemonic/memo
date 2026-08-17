from __future__ import annotations

import tomllib

from memo.agents.sandbox.guidance import effective_provider_args, guidance_digest


def test_claude_gets_guidance_and_dangerous_mode() -> None:
    args = effective_provider_args("claude", ["--model", "sonnet"])
    assert args[:3] == [
        "--append-system-prompt-file",
        "/run/memo/agent-guidance.md",
        "--dangerously-skip-permissions",
    ]
    explicit = effective_provider_args("claude", ["--permission-mode", "plan"])
    assert "--dangerously-skip-permissions" not in explicit
    tool_policy = effective_provider_args("claude", ["--allowed-tools", "Read"])
    assert "--dangerously-skip-permissions" not in tool_policy


def test_codex_global_flags_precede_resume_and_compose_instructions(monkeypatch, tmp_path) -> None:
    codex_home = tmp_path / ".codex"
    codex_home.mkdir()
    (codex_home / "config.toml").write_text('developer_instructions = "existing"\n')
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    args = effective_provider_args("codex", ["resume", "session"])
    assert args[0] == "--dangerously-bypass-approvals-and-sandbox"
    assert args[-2:] == ["resume", "session"]
    override = args[2]
    value = tomllib.loads(f"value={override.split('=', 1)[1]}")["value"]
    assert value.startswith("existing\n\n")
    assert ".memo-sandbox" in value
    assert len(guidance_digest()) == 64


def test_explicit_codex_security_option_wins() -> None:
    args = effective_provider_args("codex", ["--sandbox", "read-only"])
    assert "--dangerously-bypass-approvals-and-sandbox" not in args
    config = effective_provider_args("codex", ["-c", "approval_policy=on-request"])
    assert "--dangerously-bypass-approvals-and-sandbox" not in config
