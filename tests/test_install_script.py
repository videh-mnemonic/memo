from __future__ import annotations

import os
import subprocess
from pathlib import Path


def test_installer_stops_daemon_before_invoking_pipx(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    invocation = tmp_path / "pipx-args"
    pipx = fake_bin / "pipx"
    pipx.write_text('#!/bin/sh\nprintf \'%s\\n\' "$@" > "$PIPX_INVOCATION"\n')
    pipx.chmod(0o755)
    repository = Path(__file__).parents[1]
    environment = {
        **os.environ,
        "MEMO_HOME": str(tmp_path / "memo-home"),
        "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
        "PIPX_INVOCATION": str(invocation),
    }

    result = subprocess.run(
        [str(repository / "install")],
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == "Memo daemon is not running\n"
    assert invocation.read_text().splitlines() == ["install", "--force", "."]
