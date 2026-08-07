from __future__ import annotations

import subprocess
from pathlib import Path

from memo.identity import canonicalize_remote, discover_repo_identity, local_namespace


def test_remote_transport_equivalence() -> None:
    expected = "github.com/you/app"
    assert canonicalize_remote("git@github.com:you/app.git") == expected
    assert canonicalize_remote("https://github.com/you/app.git?x=1#fragment") == expected
    assert canonicalize_remote("ssh://git@GITHUB.COM/you/app.git") == expected


def test_nested_remote_and_port() -> None:
    assert canonicalize_remote("ssh://user@gitlab.example:2222/group/sub/repo.git") == \
        "gitlab.example:2222/group/sub/repo"


def test_credentials_and_malformed_remote() -> None:
    assert canonicalize_remote("https://name:secret@Example.COM/team/repo.git") == "example.com/team/repo"
    assert canonicalize_remote("not a remote") == ""


def test_local_namespaces_use_full_path(tmp_path: Path) -> None:
    one = tmp_path / "one" / "same"
    two = tmp_path / "two" / "same"
    one.mkdir(parents=True)
    two.mkdir(parents=True)
    assert local_namespace(one) != local_namespace(two)
    assert local_namespace(one) == local_namespace(one)
    assert len(local_namespace(one)) <= 120


def test_discovery_prefers_origin(tmp_path: Path) -> None:
    subprocess.run(["git", "init", str(tmp_path)], check=True, stdout=subprocess.PIPE)
    subprocess.run(["git", "remote", "add", "aaa", "https://example.com/a.git"], cwd=tmp_path, check=True)
    subprocess.run(["git", "remote", "add", "origin", "git@github.com:you/app.git"], cwd=tmp_path, check=True)
    identity = discover_repo_identity(tmp_path)
    assert identity.canonical_remote == "github.com/you/app"
    assert identity.archive_namespace == "github.com_you_app"
