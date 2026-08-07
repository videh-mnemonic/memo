from __future__ import annotations

import gzip
import io
import os
import subprocess
import tarfile
from pathlib import Path

from .models import SessionMeta


def git_env(meta: SessionMeta, session_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    if meta.repo_kind == "synthetic":
        env["GIT_DIR"] = str(session_dir / "git" / "synthetic.git")
        env["GIT_WORK_TREE"] = meta.repo_root
    return env


def _git(meta: SessionMeta, session_dir: Path, args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        ["git", *args], cwd=meta.repo_root, env=git_env(meta, session_dir),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check,
    )


def head(meta: SessionMeta, session_dir: Path) -> str:
    result = _git(meta, session_dir, ["rev-parse", "HEAD"], check=False)
    return result.stdout.decode().strip() if result.returncode == 0 else ""


def branch(meta: SessionMeta, session_dir: Path) -> str:
    result = _git(meta, session_dir, ["branch", "--show-current"], check=False)
    return result.stdout.decode().strip() if result.returncode == 0 else ""


def initialize_synthetic(meta: SessionMeta, session_dir: Path) -> None:
    gitdir = session_dir / "git" / "synthetic.git"
    gitdir.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "--bare", str(gitdir)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    env = git_env(meta, session_dir)
    subprocess.run(["git", "config", "user.name", "memo"], cwd=meta.repo_root, env=env, check=True)
    subprocess.run(["git", "config", "user.email", "memo@localhost"], cwd=meta.repo_root, env=env, check=True)
    subprocess.run(["git", "add", "-A"], cwd=meta.repo_root, env=env, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "memo synthetic initial state"],
        cwd=meta.repo_root, env=env, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def _write_or_remove(path: Path, data: bytes) -> None:
    if data:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    elif path.exists():
        path.unlink()


def _untracked(meta: SessionMeta, session_dir: Path) -> list[Path]:
    result = _git(meta, session_dir, ["ls-files", "--others", "--exclude-standard", "-z"])
    return [Path(os.fsdecode(item)) for item in result.stdout.split(b"\0") if item]


def _tar_untracked(root: Path, names: list[Path], destination: Path) -> None:
    if not names:
        if destination.exists():
            destination.unlink()
        return
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for relative in sorted(names, key=lambda p: p.as_posix()):
            source = root / relative
            if not source.exists() and not source.is_symlink():
                continue
            info = archive.gettarinfo(str(source), arcname=relative.as_posix())
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            if info.isfile():
                with source.open("rb") as handle:
                    archive.addfile(info, handle)
            else:
                archive.addfile(info)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as handle:
        with gzip.GzipFile(filename="", mode="wb", fileobj=handle, mtime=0) as zipped:
            zipped.write(raw.getvalue())


def snapshot_initial(meta: SessionMeta, session_dir: Path) -> str:
    (session_dir / "git").mkdir(parents=True, exist_ok=True)
    if meta.repo_kind == "synthetic":
        initialize_synthetic(meta, session_dir)
    current = head(meta, session_dir)
    if not current:
        raise RuntimeError("memo requires a repository with at least one commit")
    bundle = session_dir / "git" / "initial.bundle"
    _git(meta, session_dir, ["bundle", "create", str(bundle), "HEAD"])
    diff = _git(meta, session_dir, ["diff", "--binary", "HEAD"]).stdout
    _write_or_remove(session_dir / "git" / "initial-uncommitted.patch", diff)
    _tar_untracked(Path(meta.repo_root), _untracked(meta, session_dir), session_dir / "git" / "initial-untracked.tar.gz")
    return current


def snapshot_final(meta: SessionMeta, session_dir: Path) -> str:
    current = head(meta, session_dir)
    diff = _git(meta, session_dir, ["diff", "--binary", "HEAD"]).stdout if current else b""
    _write_or_remove(session_dir / "git" / "final-uncommitted.patch", diff)
    _tar_untracked(Path(meta.repo_root), _untracked(meta, session_dir), session_dir / "git" / "final-untracked.tar.gz")
    return current


def write_commit_patch(meta: SessionMeta, session_dir: Path, before: str, after: str, destination: Path) -> None:
    if not before or not after or before == after:
        _write_or_remove(destination, b"")
        return
    result = _git(meta, session_dir, ["format-patch", "--stdout", "--binary", f"{before}..{after}"])
    _write_or_remove(destination, result.stdout)

