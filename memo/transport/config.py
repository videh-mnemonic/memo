"""Discover typed S3 transport settings from the environment."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class S3Config:
    """Environment-derived settings for the S3 transport."""

    bucket: str
    prefix: str = "memo"
    endpoint_url: str | None = None
    region: str | None = None
    profile: str | None = None
    upload_concurrency: int = 3
    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None

    @classmethod
    def discover(cls, required: bool = False) -> S3Config | None:
        bucket = os.environ.get("MEMO_S3_BUCKET", "").strip()
        if not bucket:
            if required:
                raise ValueError("S3 transport requires MEMO_S3_BUCKET")
            return None
        concurrency = int(os.environ.get("MEMO_S3_UPLOAD_CONCURRENCY", "3"))
        if concurrency <= 0:
            raise ValueError("MEMO_S3_UPLOAD_CONCURRENCY must be positive")
        access_key = os.environ.get("AWS_ACCESS_KEY_ID") or None
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or None
        if bool(access_key) != bool(secret_key):
            raise ValueError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together")
        return cls(
            bucket=bucket,
            prefix=os.environ.get("MEMO_S3_PREFIX", "memo").strip("/"),
            endpoint_url=os.environ.get("MEMO_S3_ENDPOINT") or None,
            region=os.environ.get("MEMO_S3_REGION") or None,
            profile=os.environ.get("MEMO_S3_PROFILE") or None,
            upload_concurrency=concurrency,
            access_key=access_key,
            secret_key=secret_key,
            session_token=os.environ.get("AWS_SESSION_TOKEN")
            or os.environ.get("AWS_SECURITY_TOKEN")
            or None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bucket": self.bucket,
            "prefix": self.prefix,
            "endpoint_url": self.endpoint_url,
            "region": self.region,
            "profile": self.profile,
            "upload_concurrency": self.upload_concurrency,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
            "session_token": self.session_token,
        }

    @classmethod
    def from_dict(cls, values: Mapping[str, Any]) -> S3Config:
        def optional_string(name: str) -> str | None:
            value = values.get(name)
            if value is None:
                return None
            if not isinstance(value, str):
                raise ValueError(f"S3 config field must be a string: {name}")
            return value

        bucket = values.get("bucket")
        if not isinstance(bucket, str) or not bucket.strip():
            raise ValueError("S3 config requires bucket")
        concurrency = values.get("upload_concurrency", 3)
        if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency <= 0:
            raise ValueError("S3 config upload_concurrency must be a positive integer")
        access_key = optional_string("access_key")
        secret_key = optional_string("secret_key")
        if bool(access_key) != bool(secret_key):
            raise ValueError("S3 config access_key and secret_key must be set together")
        prefix = optional_string("prefix")
        return cls(
            bucket=bucket.strip(),
            prefix=("memo" if prefix is None else prefix.strip("/")),
            endpoint_url=optional_string("endpoint_url"),
            region=optional_string("region"),
            profile=optional_string("profile"),
            upload_concurrency=concurrency,
            access_key=access_key,
            secret_key=secret_key,
            session_token=optional_string("session_token"),
        )
