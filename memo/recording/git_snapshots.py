"""Git-backed storage for filesystem snapshot trees."""

from __future__ import annotations

import os
import subprocess
import tarfile
import tempfile
from pathlib import Path


class GitSnapshotError(RuntimeError):
    """Raised when a filesystem snapshot cannot be stored or restored."""


class GitSnapshotStore:
    def __init__(self, path: Path):
        self.path = path

    def initialize(self) -> None:
        if (self.path / "HEAD").is_file() and (self.path / "objects").is_dir():
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._run("init", "--bare", "--quiet", str(self.path), cwd=self.path.parent)

    def commit(self, tree: Path, parent: str | None, message: str) -> str:
        self.initialize()
        index_fd, index_name = tempfile.mkstemp(prefix="git-index-", dir=tree.parent)
        os.close(index_fd)
        os.unlink(index_name)
        try:
            environment = {**os.environ, "GIT_INDEX_FILE": index_name}
            self._run(
                "--git-dir",
                str(self.path),
                "--work-tree",
                str(tree),
                "add",
                "--all",
                ".",
                env=environment,
            )
            tree_id = self._run(
                "--git-dir", str(self.path), "write-tree", env=environment
            ).stdout.strip()
            command = ["--git-dir", str(self.path), "commit-tree", tree_id, "-m", message]
            if parent:
                command[4:4] = ["-p", parent]
            result = self._run(
                *command,
                env={
                    **environment,
                    "GIT_AUTHOR_NAME": "Memo",
                    "GIT_AUTHOR_EMAIL": "memo@localhost",
                    "GIT_COMMITTER_NAME": "Memo",
                    "GIT_COMMITTER_EMAIL": "memo@localhost",
                },
            )
        finally:
            Path(index_name).unlink(missing_ok=True)
        return result.stdout.strip()

    def contains(self, commit: str) -> bool:
        if not self.path.is_dir():
            return False
        result = subprocess.run(
            ["git", "--git-dir", str(self.path), "cat-file", "-e", f"{commit}^{{commit}}"],
            capture_output=True,
            check=False,
            text=True,
        )
        return result.returncode == 0

    def restore(self, commit: str, destination: Path) -> None:
        if not self.contains(commit):
            raise GitSnapshotError(f"snapshot commit is missing: {commit}")
        destination.mkdir(parents=True, exist_ok=True)
        entries = self._run(
            "--git-dir", str(self.path), "ls-tree", "-r", "--name-only", commit
        ).stdout
        if not entries:
            return
        process = subprocess.Popen(
            ["git", "--git-dir", str(self.path), "archive", "--format=tar", commit],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdout is not None
        try:
            with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
                for member in archive:
                    if member.issym() or member.islnk() or member.isdev():
                        raise GitSnapshotError(f"unsupported snapshot entry: {member.name}")
                    target = (destination / member.name).resolve()
                    target.relative_to(destination.resolve())
                    archive.extract(member, destination)
        except GitSnapshotError:
            process.kill()
            process.wait()
            raise
        except (OSError, tarfile.TarError, ValueError) as error:
            process.kill()
            process.wait()
            raise GitSnapshotError(f"could not restore snapshot {commit}") from error
        stderr = process.stderr.read().decode(errors="replace") if process.stderr else ""
        if process.wait() != 0:
            raise GitSnapshotError(stderr.strip() or f"could not restore snapshot {commit}")

    @staticmethod
    def _run(*arguments: str, cwd: Path | None = None, env: dict[str, str] | None = None):
        try:
            result = subprocess.run(
                ["git", *arguments],
                cwd=cwd,
                env=env,
                capture_output=True,
                check=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            detail = getattr(error, "stderr", "") or str(error)
            raise GitSnapshotError(f"git snapshot operation failed: {detail.strip()}") from error
        return result
