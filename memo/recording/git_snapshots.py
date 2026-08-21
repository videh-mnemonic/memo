"""Git-backed storage for filesystem snapshot trees."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
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
        self._run(
            "init",
            "--bare",
            "--quiet",
            "--object-format=sha1",
            str(self.path),
            cwd=self.path.parent,
        )

    def commit(self, tree: Path, parent: str | None, message: str) -> str:
        tree_id = self.write_tree(tree)
        return self.commit_tree(tree_id, parent, message)

    def write_tree(self, tree: Path) -> str:
        """Store every regular file literally and return its content-addressed tree ID.

        Ordinary ``git add`` is intentionally not used here. It consults ignore
        rules and clean filters, and treats embedded repositories as gitlinks;
        all three behaviours can silently make a recorded snapshot differ from
        the filesystem bytes Memo was asked to preserve.
        """
        self.initialize()
        root = tree.resolve()
        files: list[tuple[tuple[bytes, ...], Path, int]] = []
        for path in sorted(tree.rglob("*"), key=lambda item: os.fsencode(item.as_posix())):
            metadata = path.lstat()
            if stat.S_ISDIR(metadata.st_mode):
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise GitSnapshotError(f"unsupported snapshot entry: {path.relative_to(tree)}")
            resolved = path.resolve()
            resolved.relative_to(root)
            relative = path.relative_to(tree)
            parts = tuple(os.fsencode(part) for part in relative.parts)
            mode = 0o100755 if metadata.st_mode & 0o111 else 0o100644
            files.append((parts, path, mode))

        object_ids = self._hash_files_literally(tree, [path for _, path, _ in files])
        directories: dict[tuple[bytes, ...], list[tuple[int, bytes, str, bytes]]] = {}
        for (parts, _path, mode), object_id in zip(files, object_ids, strict=True):
            directories.setdefault(parts[:-1], []).append((mode, b"blob", object_id, parts[-1]))
            for depth in range(len(parts)):
                directories.setdefault(parts[:depth], [])

        if not directories:
            return self._run("--git-dir", str(self.path), "mktree").stdout.strip()

        tree_ids: dict[tuple[bytes, ...], str] = {}
        for depth in range(max(map(len, directories)), -1, -1):
            at_depth = sorted(directory for directory in directories if len(directory) == depth)
            payloads = []
            for directory in at_depth:
                entries = list(directories[directory])
                children = {
                    child[: len(directory) + 1]
                    for child in directories
                    if len(child) > len(directory) and child[: len(directory)] == directory
                }
                for child in children:
                    child_id = tree_ids.get(child)
                    if child_id is not None:
                        entries.append((0o40000, b"tree", child_id, child[-1]))
                payloads.append(
                    b"".join(
                        f"{mode:o} ".encode()
                        + kind
                        + b" "
                        + object_id.encode()
                        + b"\t"
                        + name
                        + b"\0"
                        for mode, kind, object_id, name in entries
                    )
                    + b"\0"
                )
            output = self._run_bytes(
                "--git-dir",
                str(self.path),
                "mktree",
                "-z",
                "--batch",
                input_data=b"".join(payloads),
            )
            object_ids = output.decode().splitlines()
            if len(object_ids) != len(at_depth):
                raise GitSnapshotError("git snapshot operation returned an incomplete tree list")
            tree_ids.update(zip(at_depth, object_ids, strict=True))
        return tree_ids[()]

    def _hash_files_literally(self, root: Path, files: list[Path]) -> list[str]:
        """Hash paths without attributes, newline conversion, or clean filters."""
        if not files:
            return []
        ordinary: list[tuple[int, bytes]] = []
        result: list[str | None] = [None] * len(files)
        for index, path in enumerate(files):
            relative = os.fsencode(path.relative_to(root))
            if b"\n" in relative or b"\r" in relative:
                output = self._run_bytes(
                    "--git-dir",
                    str(self.path),
                    "hash-object",
                    "-w",
                    "--no-filters",
                    "--",
                    relative,
                    cwd=root,
                )
                result[index] = output.decode().strip()
            else:
                ordinary.append((index, relative))
        if ordinary:
            output = (
                self._run_bytes(
                    "--git-dir",
                    str(self.path),
                    "hash-object",
                    "-w",
                    "--no-filters",
                    "--stdin-paths",
                    cwd=root,
                    input_data=b"".join(path + b"\n" for _, path in ordinary),
                )
                .decode()
                .splitlines()
            )
            if len(output) != len(ordinary):
                raise GitSnapshotError("git snapshot operation returned an incomplete object list")
            for (index, _), object_id in zip(ordinary, output, strict=True):
                result[index] = object_id
        if any(object_id is None for object_id in result):
            raise GitSnapshotError("git snapshot operation did not store every file")
        return [object_id for object_id in result if object_id is not None]

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

    def tree_ids(self, commits: Iterable[str]) -> dict[str, str]:
        """Resolve many commit tree IDs in one Git process."""
        wanted = list(dict.fromkeys(commits))
        if not wanted or not self.path.is_dir():
            return {}
        result = subprocess.run(
            [
                "git",
                "--git-dir",
                str(self.path),
                "cat-file",
                "--batch-check=%(objectname) %(objecttype)",
            ],
            input="".join(f"{commit}^{{tree}}\n" for commit in wanted),
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode != 0:
            return {}
        trees: dict[str, str] = {}
        for commit, line in zip(wanted, result.stdout.splitlines(), strict=False):
            fields = line.split()
            if len(fields) == 2 and fields[1] == "tree":
                trees[commit] = fields[0]
        return trees

    def reachable_from(self, commit: str) -> set[str]:
        """Return every commit carried by a bundle rooted at ``commit``."""
        if not self.contains(commit):
            return set()
        result = self._run("--git-dir", str(self.path), "rev-list", commit)
        return set(result.stdout.splitlines())

    def check_connectivity(self, commit: str) -> None:
        """Require the selected history to have all referenced Git objects."""
        if not self.contains(commit):
            raise GitSnapshotError(f"snapshot commit is missing: {commit}")
        self._run(
            "--git-dir",
            str(self.path),
            "fsck",
            "--connectivity-only",
            "--no-dangling",
            commit,
        )

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

    def import_objects(
        self,
        source: Path,
        namespace: str,
        required_commits: Iterable[str] = (),
    ) -> str:
        """Fetch one snapshot ref without changing the published snapshot ref."""
        self.initialize()
        if source.is_file():
            fields = self._run("bundle", "list-heads", str(source), self.REF).stdout.split()
            if len(fields) != 2 or fields[1] != self.REF:
                raise GitSnapshotError("snapshot recovery bundle is missing its published ref")
            source_ref = fields[0]
        else:
            source_ref = self._run(
                "--git-dir", str(source), "rev-parse", "--verify", self.REF
            ).stdout.strip()
        safe_namespace = "".join(
            character if character.isalnum() or character in {"-", "_"} else "-"
            for character in namespace
        )
        recovery_root = f"refs/memo/recovery/{safe_namespace}"
        recovery_ref = f"{recovery_root}/tip"
        self._run(
            "--git-dir",
            str(self.path),
            "fetch",
            "--quiet",
            "--no-tags",
            str(source),
            f"{self.REF}:{recovery_ref}",
        )
        actual = self._run(
            "--git-dir", str(self.path), "rev-parse", "--verify", recovery_ref
        ).stdout.strip()
        if actual != source_ref:
            raise GitSnapshotError(
                f"snapshot recovery tip mismatch: expected {source_ref}, received {actual}"
            )
        required = list(dict.fromkeys(required_commits))
        if source.is_dir():
            available = GitSnapshotStore(source).contains_many(required)
            remaining = [
                commit for commit in required if commit in available and not self.contains(commit)
            ]
            for offset in range(0, len(remaining), 128):
                chunk = remaining[offset : offset + 128]
                refspecs = [
                    f"{commit}:{recovery_root}/objects/{offset + index:08d}"
                    for index, commit in enumerate(chunk)
                ]
                self._run(
                    "--git-dir",
                    str(self.path),
                    "fetch",
                    "--quiet",
                    "--no-tags",
                    str(source),
                    *refspecs,
                )
        return actual

    def restore(self, commit: str, destination: Path) -> None:
        if not self.contains(commit):
            raise GitSnapshotError(f"snapshot commit is missing: {commit}")
        destination.mkdir(parents=True, exist_ok=True)
        raw_entries = self._run_bytes(
            "--git-dir", str(self.path), "ls-tree", "-r", "-z", "--full-tree", commit
        )
        if not raw_entries:
            return
        entries: list[tuple[str, int, Path]] = []
        root = destination.resolve()
        for raw_entry in raw_entries.rstrip(b"\0").split(b"\0"):
            metadata, separator, raw_name = raw_entry.partition(b"\t")
            fields = metadata.split()
            if not separator or len(fields) != 3:
                raise GitSnapshotError("snapshot tree contains an invalid entry")
            raw_mode, kind, raw_object_id = fields
            if kind != b"blob" or raw_mode not in {b"100644", b"100755"}:
                raise GitSnapshotError(f"unsupported snapshot entry: {os.fsdecode(raw_name)}")
            relative = Path(os.fsdecode(raw_name))
            if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
                raise GitSnapshotError(f"unsafe snapshot entry: {os.fsdecode(raw_name)}")
            target = destination / relative
            try:
                target.resolve().relative_to(root)
            except ValueError as error:
                raise GitSnapshotError(f"unsafe snapshot entry: {os.fsdecode(raw_name)}") from error
            entries.append((raw_object_id.decode(), int(raw_mode, 8), target))

        process = subprocess.Popen(
            ["git", "--git-dir", str(self.path), "cat-file", "--batch"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert process.stdin is not None
        assert process.stdout is not None
        try:
            for object_id, mode, target in entries:
                process.stdin.write(f"{object_id}\n".encode())
                process.stdin.flush()
                header = process.stdout.readline().decode(errors="replace").strip().split()
                if len(header) != 3 or header[0] != object_id or header[1] != "blob":
                    raise GitSnapshotError(f"snapshot blob is missing or invalid: {object_id}")
                size = int(header[2])
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    raise GitSnapshotError(
                        f"duplicate snapshot entry: {target.relative_to(destination)}"
                    )
                remaining = size
                with target.open("xb") as handle:
                    while remaining:
                        chunk = process.stdout.read(min(1024 * 1024, remaining))
                        if not chunk:
                            raise GitSnapshotError(f"snapshot blob is truncated: {object_id}")
                        handle.write(chunk)
                        remaining -= len(chunk)
                if process.stdout.read(1) != b"\n":
                    raise GitSnapshotError(f"snapshot blob has invalid framing: {object_id}")
                target.chmod(0o755 if mode == 0o100755 else 0o644)
            process.stdin.close()
        except GitSnapshotError:
            process.kill()
            process.wait()
            raise
        except (OSError, ValueError) as error:
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

    @staticmethod
    def _run_bytes(
        *arguments: str | bytes,
        cwd: Path | None = None,
        input_data: bytes | None = None,
    ) -> bytes:
        try:
            result = subprocess.run(
                [os.fsencode("git"), *(os.fsencode(value) for value in arguments)],
                cwd=cwd,
                input=input_data,
                capture_output=True,
                check=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            stderr = getattr(error, "stderr", b"")
            detail = os.fsdecode(stderr).strip() if stderr else str(error)
            raise GitSnapshotError(f"git snapshot operation failed: {detail}") from error
        return result.stdout
