from __future__ import annotations

import hashlib
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit

from .config import NAMESPACE_MAX_LENGTH


@dataclass(frozen=True)
class RepoIdentity:
    repo_kind: str
    repo_root: Path
    repo_name: str
    remote: str
    canonical_remote: str
    archive_namespace: str


def _git(args: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=check,
    )


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-") or "repo"
    slug = re.sub(r"_+", "_", slug)
    if len(slug) > NAMESPACE_MAX_LENGTH:
        digest = hashlib.sha256(value.encode()).hexdigest()[:12]
        slug = f"{slug[:NAMESPACE_MAX_LENGTH - 13]}_{digest}"
    return slug


def canonicalize_remote(remote: str) -> str:
    value = remote.strip()
    if not value or any(ord(c) < 32 for c in value):
        return ""
    # Git's scp-like form: [user@]host:path. Avoid treating Windows paths as remotes.
    match = None if "://" in value else re.fullmatch(r"(?:[^/@:]+@)?([^/:]+):(.+)", value)
    if match and not re.match(r"^[A-Za-z]:[\\/]", value):
        host, path = match.groups()
        port = ""
    else:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https", "ssh", "git"} or not parsed.hostname:
            return ""
        host = parsed.hostname
        port = f":{parsed.port}" if parsed.port else ""
        path = parsed.path
    host = host.lower().strip(".") + port
    path = re.sub(r"/+", "/", unquote(path)).strip("/")
    if path.lower().endswith(".git"):
        path = path[:-4]
    if not host or not path or path in {".", ".."}:
        return ""
    return f"{host}/{path}"


def remote_namespace(canonical_remote: str) -> str:
    if not canonical_remote:
        raise ValueError("canonical remote is empty")
    return _safe_slug(canonical_remote.replace("/", "_").replace(":", "_"))


def local_namespace(repo_root: Path) -> str:
    root = repo_root.resolve()
    digest = hashlib.sha256(str(root).encode("utf-8")).hexdigest()[:10]
    return _safe_slug(f"local_{root.name or 'root'}_{digest}")


def select_git_remote(repo_root: Path) -> str | None:
    result = _git(["remote"], repo_root, check=False)
    if result.returncode:
        return None
    names = sorted(filter(None, result.stdout.splitlines()))
    ordered = (["origin"] if "origin" in names else []) + [n for n in names if n != "origin"]
    for name in ordered:
        url = _git(["remote", "get-url", name], repo_root, check=False)
        candidate = url.stdout.strip() if url.returncode == 0 else ""
        if canonicalize_remote(candidate):
            return candidate
    return None


def discover_repo_identity(cwd: Path) -> RepoIdentity:
    cwd = cwd.resolve()
    top = _git(["rev-parse", "--show-toplevel"], cwd, check=False)
    if top.returncode == 0:
        root = Path(top.stdout.strip()).resolve()
        kind = "real"
        remote = select_git_remote(root) or ""
    else:
        root, kind, remote = cwd, "synthetic", ""
    canonical = canonicalize_remote(remote) if remote else ""
    namespace = remote_namespace(canonical) if canonical else local_namespace(root)
    return RepoIdentity(kind, root, root.name or "root", remote, canonical, namespace)
