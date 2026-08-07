from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Leg:
    leg_id: str
    tool_args: list[str]
    start_utc: str
    end_utc: str | None = None
    exit_code: int | None = None
    trace_file: str | None = None
    complete: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Leg":
        return cls(**value)


@dataclass
class SessionMeta:
    session_id: str
    tool: str
    repo_kind: str
    repo_root: str
    repo_name: str
    remote: str
    canonical_remote: str
    archive_namespace: str
    initial_head: str
    final_head: str
    first_seen_utc: str
    last_activity_utc: str
    legs: list[Leg] = field(default_factory=list)
    resumes: str | None = None
    coverage: str = "full"
    shipped: bool = False
    shipped_at: str | None = None
    archive_sha256: str | None = None
    branch: str = ""

    def validate(self) -> None:
        if self.repo_kind not in {"real", "synthetic"}:
            raise ValueError(f"invalid repo_kind: {self.repo_kind}")
        ns = self.archive_namespace
        if not ns or ns in {".", ".."} or "/" in ns or "\\" in ns or any(ord(c) < 32 for c in ns):
            raise ValueError("archive_namespace must be a safe, non-empty path component")
        if not self.remote and self.canonical_remote:
            raise ValueError("canonical_remote requires remote")
        if self.coverage not in {"full", "partial_outside_repo"}:
            raise ValueError(f"invalid coverage: {self.coverage}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SessionMeta":
        value = dict(value)
        value["legs"] = [Leg.from_dict(v) for v in value.get("legs", [])]
        return cls(**value)

    @classmethod
    def load(cls, path: Path) -> "SessionMeta":
        result = cls.from_dict(json.loads(path.read_text()))
        result.validate()
        return result

    def save(self, path: Path) -> None:
        self.validate()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n")
        temporary.replace(path)

