from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

STEP_INTERVAL_SECONDS = 15.0
MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024
WATCHER_DEBOUNCE_SECONDS = 0.25
PUSH_INTERVAL_SECONDS = 15 * 60.0


@dataclass(frozen=True)
class Paths:
    home: Path
    archive: Path | None = None
    runtime: Path | None = None
    socket: Path | None = None
    registry: Path | None = None
    spool: Path | None = None

    def __post_init__(self) -> None:
        archive = self.archive or self.home / "archive"
        runtime = self.runtime or self.home / "runtime"
        object.__setattr__(self, "archive", archive)
        object.__setattr__(self, "runtime", runtime)
        object.__setattr__(self, "socket", self.socket or runtime / "memo.sock")
        object.__setattr__(self, "registry", self.registry or runtime / "registry.sqlite")
        object.__setattr__(self, "spool", self.spool or runtime / "sessions")

    @classmethod
    def discover(cls) -> "Paths":
        home = Path(os.environ.get("MEMO_HOME", "~/memo")).expanduser().resolve()
        return cls(home)

    def ensure_storage(self) -> None:
        assert self.archive is not None
        self.archive.mkdir(parents=True, exist_ok=True)
        assert self.runtime is not None
        assert self.spool is not None
        self.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.spool.mkdir(parents=True, exist_ok=True, mode=0o700)


@dataclass(frozen=True)
class TransportConfig:
    bucket: str
    prefix: str = "memo"
    endpoint_url: str | None = None
    region: str | None = None
    profile: str | None = None

    @classmethod
    def discover(cls, required: bool = False) -> "TransportConfig | None":
        bucket = os.environ.get("MEMO_S3_BUCKET", "").strip()
        if not bucket:
            if required:
                raise ValueError("S3 transport requires MEMO_S3_BUCKET")
            return None
        return cls(
            bucket=bucket,
            prefix=os.environ.get("MEMO_S3_PREFIX", "memo").strip("/"),
            endpoint_url=os.environ.get("MEMO_S3_ENDPOINT") or None,
            region=os.environ.get("MEMO_S3_REGION") or None,
            profile=os.environ.get("MEMO_S3_PROFILE") or None,
        )

    def client(self):
        import boto3
        session = boto3.Session(profile_name=self.profile, region_name=self.region)
        return session.client("s3", endpoint_url=self.endpoint_url)
