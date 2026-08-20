"""Keep the suite hermetic: no test may reach a real archive or a real bucket."""

from __future__ import annotations

from pathlib import Path

import pytest

#: Settings that would point Memo at a real archive or a real S3 account. Tests
#: start daemons that inherit the environment, and a daemon that finds live
#: credentials will push recordings to whatever bucket they authorise.
INHERITED_SETTINGS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_DEFAULT_PROFILE",
    "AWS_DEFAULT_REGION",
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "MEMO_HOME",
    "MEMO_LARGE_ARCHIVE_BYTES",
    "MEMO_S3_BUCKET",
    "MEMO_S3_ENDPOINT",
    "MEMO_S3_PREFIX",
    "MEMO_S3_PROFILE",
    "MEMO_S3_REGION",
    "MEMO_S3_UPLOAD_CONCURRENCY",
)

#: Most commands refuse to run without a bucket, so the suite needs one. This
#: name cannot resolve, so an upload nobody intended fails instead of landing in
#: a real archive.
UNREACHABLE_BUCKET = "memo-tests-no-such-bucket"


@pytest.fixture(autouse=True)
def isolated_memo_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> Path:
    """Give every test its own archive and a bucket that cannot be reached."""
    for name in INHERITED_SETTINGS:
        monkeypatch.delenv(name, raising=False)
    home = tmp_path_factory.mktemp("memo-home")
    monkeypatch.setenv("MEMO_HOME", str(home))
    monkeypatch.setenv("MEMO_S3_BUCKET", UNREACHABLE_BUCKET)
    return home
