"""Discover typed S3 transport settings from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class S3Config:
    """Environment-derived settings for the S3 transport."""

    bucket: str
    prefix: str = "memo"
    endpoint_url: str | None = None
    region: str | None = None
    profile: str | None = None
    upload_concurrency: int = 3

    @classmethod
    def discover(cls, required: bool = False) -> "S3Config | None":
        bucket = os.environ.get("MEMO_S3_BUCKET", "").strip()
        if not bucket:
            if required:
                raise ValueError("S3 transport requires MEMO_S3_BUCKET")
            return None
        concurrency = int(os.environ.get("MEMO_S3_UPLOAD_CONCURRENCY", "3"))
        if concurrency <= 0:
            raise ValueError("MEMO_S3_UPLOAD_CONCURRENCY must be positive")
        return cls(
            bucket=bucket,
            prefix=os.environ.get("MEMO_S3_PREFIX", "memo").strip("/"),
            endpoint_url=os.environ.get("MEMO_S3_ENDPOINT") or None,
            region=os.environ.get("MEMO_S3_REGION") or None,
            profile=os.environ.get("MEMO_S3_PROFILE") or None,
            upload_concurrency=concurrency,
        )
