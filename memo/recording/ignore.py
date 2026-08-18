"""Apply Git-compatible ignore rules and Memo exclusions during snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pathspec.gitignore import GitIgnoreSpec

from .paths import StoragePaths


@dataclass(frozen=True)
class IgnoreDecision:
    ignored: bool
    source: str | None = None


class IgnorePolicy:
    """Repository-scoped Git ignore rules with Memo self-exclusion."""

    def __init__(self, root: Path, paths: StoragePaths | None = None):
        self.root = root.resolve()
        self.paths = paths
        self._cache: dict[Path, list[tuple[Path, str, GitIgnoreSpec]]] = {}
        self._excluded = self._self_excluded_roots()

    def _self_excluded_roots(self) -> list[Path]:
        if self.paths is None:
            return []
        candidates = [self.paths.runtime, self.paths.archive]
        return [
            path.resolve()
            for path in candidates
            if path is not None and path.resolve().is_relative_to(self.root)
        ]

    @staticmethod
    def _is_repository(path: Path) -> bool:
        marker = path / ".git"
        return marker.is_dir() or marker.is_file()

    def _repository_root(self, path: Path) -> Path:
        relative = path.resolve().relative_to(self.root)
        current = self.root
        repository = self.root
        if self._is_repository(current):
            repository = current
        for part in relative.parts:
            current /= part
            if self._is_repository(current):
                repository = current
        return repository

    def _specs(self, parent: Path) -> list[tuple[Path, str, GitIgnoreSpec]]:
        parent = parent.resolve()
        cached = self._cache.get(parent)
        if cached is not None:
            return cached
        result: list[tuple[Path, str, GitIgnoreSpec]] = []
        repository = self._repository_root(parent)
        relative = parent.relative_to(repository)
        current = repository
        directories = [repository]
        for part in relative.parts:
            current /= part
            directories.append(current)
        for directory in directories:
            filename = ".gitignore"
            ignore_file = directory / filename
            try:
                lines = ignore_file.read_text(errors="surrogateescape").splitlines()
            except (FileNotFoundError, NotADirectoryError, PermissionError):
                continue
            result.append((directory, filename, GitIgnoreSpec.from_lines(lines)))
        self._cache[parent] = result
        return result

    def decision(self, path: Path, is_dir: bool = False) -> IgnoreDecision:
        if path.name == ".git":
            return IgnoreDecision(True, "git-metadata")
        if path.absolute() == self.root / ".memo-sandbox":
            return IgnoreDecision(True, "memo-control")
        absolute = path.resolve(strict=False)
        for excluded in self._excluded:
            if absolute == excluded or absolute.is_relative_to(excluded):
                return IgnoreDecision(True, "memo-storage")
        ignored = False
        source = None
        specs_parent = path if is_dir and self._is_repository(path) else path.parent
        for base_path, filename, spec in self._specs(specs_parent):
            try:
                candidate = path.relative_to(base_path).as_posix()
            except ValueError:
                continue
            if is_dir:
                candidate += "/"
            result = spec.check_file(candidate)
            if result.include is not None:
                ignored = bool(result.include)
                source = (base_path / filename).relative_to(self.root).as_posix()
        return IgnoreDecision(ignored, source if ignored else None)
