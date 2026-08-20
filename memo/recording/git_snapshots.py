"""Git-backed storage for filesystem snapshot trees."""

from __future__ import annotations

import os
import shutil
import subprocess
import tarfile
import tempfile
from collections.abc import Iterable
from pathlib import Path


class GitSnapshotError(RuntimeError):
    """Raised when a filesystem snapshot cannot be stored or restored."""


class GitSnapshotStore:
    REF = "refs/heads/master"

    def __init__(self, path: Path):
        self.path = path

    def initialize(self) -> None:
        if (self.path / "HEAD").is_file() and (self.path / "objects").is_dir():
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._run("init", "--bare", "--quiet", str(self.path), cwd=self.path.parent)

    def commit(self, tree: Path, parent: str | None, message: str) -> str:
        tree_id = self.write_tree(tree)
        return self.commit_tree(tree_id, parent, message)

    def write_tree(self, tree: Path) -> str:
        """Store a filesystem tree and return its content-addressed tree ID."""
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
            return self._run(
                "--git-dir", str(self.path), "write-tree", env=environment
            ).stdout.strip()
        finally:
            Path(index_name).unlink(missing_ok=True)

    def commit_tree(self, tree_id: str, parent: str | None, message: str) -> str:
        """Commit a previously written tree and advance the snapshot ref."""
        self.initialize()
        environment = {
            **os.environ,
            "GIT_AUTHOR_NAME": "Memo",
            "GIT_AUTHOR_EMAIL": "memo@localhost",
            "GIT_COMMITTER_NAME": "Memo",
            "GIT_COMMITTER_EMAIL": "memo@localhost",
        }
        command = ["--git-dir", str(self.path), "commit-tree", tree_id, "-m", message]
        if parent:
            command[4:4] = ["-p", parent]
        result = self._run(*command, env=environment)
        commit = result.stdout.strip()
        self._run("--git-dir", str(self.path), "update-ref", self.REF, commit)
        return commit

    def tree_id(self, commit: str) -> str:
        """Return the tree ID referenced by a snapshot commit."""
        if not self.contains(commit):
            raise GitSnapshotError(f"snapshot commit is missing: {commit}")
        return self._run(
            "--git-dir", str(self.path), "rev-parse", f"{commit}^{{tree}}"
        ).stdout.strip()

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

    def contains_many(self, commits: Iterable[str]) -> set[str]:
        """Return which of ``commits`` this repository holds, in one Git invocation.

        Validating a long session means asking about tens of thousands of
        commits; one process per commit dominates the cost, so resolve them as a
        single batch.
        """
        wanted = list(dict.fromkeys(commits))
        if not wanted or not self.path.is_dir():
            return set()
        result = subprocess.run(
            ["git", "--git-dir", str(self.path), "cat-file", "--batch-check"],
            input="".join(f"{commit}^{{commit}}\n" for commit in wanted),
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode != 0:
            return set()
        present = set()
        for commit, line in zip(wanted, result.stdout.splitlines(), strict=False):
            # Present objects report "<sha> <type> <size>"; missing ones report
            # the queried name followed by "missing".
            fields = line.split()
            if len(fields) == 3 and fields[1] == "commit":
                present.add(commit)
        return present

    def pin(self, commit: str) -> None:
        """Keep the current snapshot reachable from the repository's branch ref."""
        if not self.contains(commit):
            raise GitSnapshotError(f"snapshot commit is missing: {commit}")
        current = subprocess.run(
            ["git", "--git-dir", str(self.path), "rev-parse", "--verify", self.REF],
            capture_output=True,
            check=False,
            text=True,
        )
        if current.returncode == 0:
            current_commit = current.stdout.strip()
            if current_commit == commit:
                return
            ancestor = subprocess.run(
                [
                    "git",
                    "--git-dir",
                    str(self.path),
                    "merge-base",
                    "--is-ancestor",
                    current_commit,
                    commit,
                ],
                capture_output=True,
                check=False,
            )
            if ancestor.returncode != 0:
                return
        self._run("--git-dir", str(self.path), "update-ref", self.REF, commit)

    def create_bundle(self, commit: str, target: Path) -> None:
        """Create a compact, self-contained bundle for one published commit."""
        if not self.contains(commit):
            raise GitSnapshotError(f"snapshot commit is missing: {commit}")
        if target.exists():
            raise FileExistsError(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with tempfile.TemporaryDirectory(
                prefix="bundle-repository-", dir=target.parent
            ) as name:
                repository = Path(name)
                self._run("init", "--bare", "--quiet", str(repository), cwd=target.parent)
                environment = {
                    **os.environ,
                    "GIT_ALTERNATE_OBJECT_DIRECTORIES": str((self.path / "objects").resolve()),
                }
                self._run(
                    "--git-dir",
                    str(repository),
                    "update-ref",
                    self.REF,
                    commit,
                    env=environment,
                )
                self._run(
                    "-c",
                    "pack.window=10",
                    "-c",
                    "pack.depth=50",
                    "-c",
                    "pack.threads=1",
                    "-c",
                    "pack.windowMemory=64m",
                    "-c",
                    "pack.compression=0",
                    "-c",
                    "pack.useSparse=true",
                    "-c",
                    "core.bigFileThreshold=32m",
                    "--git-dir",
                    str(repository),
                    "bundle",
                    "create",
                    str(target),
                    self.REF,
                    env=environment,
                )
        except BaseException:
            target.unlink(missing_ok=True)
            raise

    def import_bundle(self, bundle: Path, expected_commit: str) -> None:
        """Install a self-contained snapshot bundle as this bare repository."""
        if self.path.exists():
            raise FileExistsError(self.path)
        self.initialize()
        try:
            self._run("--git-dir", str(self.path), "bundle", "verify", str(bundle))
            self._run(
                "--git-dir",
                str(self.path),
                "fetch",
                "--quiet",
                "--no-tags",
                str(bundle),
                f"{self.REF}:{self.REF}",
            )
            actual = self._run(
                "--git-dir", str(self.path), "rev-parse", "--verify", self.REF
            ).stdout.strip()
            if actual != expected_commit:
                raise GitSnapshotError(
                    f"snapshot bundle tip mismatch: expected {expected_commit}, received {actual}"
                )
            self._run(
                "--git-dir",
                str(self.path),
                "fsck",
                "--connectivity-only",
                "--no-dangling",
                expected_commit,
            )
        except BaseException:
            shutil.rmtree(self.path, ignore_errors=True)
            raise

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
