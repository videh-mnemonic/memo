"""Provide a small MinIO-backed client for generic S3 object operations."""

from __future__ import annotations

import io
from collections.abc import Iterator
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlsplit

from .config import S3Config

MULTIPART_PART_SIZE = 8 * 1024 * 1024
METADATA_SIZE_LIMIT = 1024 * 1024
STREAM_READ_SIZE = 64 * 1024


def _error_code(error: BaseException) -> str | None:
    code = getattr(error, "code", None)
    if code is not None:
        return str(code)
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        details = response.get("Error")
        if isinstance(details, dict) and details.get("Code") is not None:
            return str(details["Code"])
    return None


def _is_not_found(error: BaseException) -> bool:
    return isinstance(error, KeyError) or _error_code(error) in {
        "NoSuchKey",
        "NoSuchObject",
        "NotFound",
        "404",
    }


def _close_response(response: Any) -> None:
    try:
        close = getattr(response, "close", None)
        if close is not None:
            close()
    finally:
        release = getattr(response, "release_conn", None)
        if release is not None:
            release()


def _minio_client(config: S3Config) -> Any:
    from minio import Minio
    from minio.credentials import (
        AWSConfigProvider,
        ChainedProvider,
        EnvAWSProvider,
        IamAwsProvider,
        StaticProvider,
    )

    endpoint = config.endpoint_url or "https://s3.amazonaws.com"
    parsed = urlsplit(endpoint if "://" in endpoint else f"https://{endpoint}")
    if not parsed.netloc:
        raise ValueError(f"invalid S3 endpoint: {endpoint}")
    if config.access_key and config.secret_key:
        credentials = StaticProvider(config.access_key, config.secret_key, config.session_token)
    else:
        providers = [EnvAWSProvider()]
        providers.append(AWSConfigProvider(profile=config.profile))
        providers.append(IamAwsProvider())
        credentials = ChainedProvider(providers)
    return Minio(
        parsed.netloc,
        secure=parsed.scheme != "http",
        region=config.region,
        credentials=credentials,
    )


class S3Store:
    """Expose the small subset of object storage operations Memo needs."""

    def __init__(self, config: S3Config, client: Any | None = None) -> None:
        self.config = config
        self.client = client or _minio_client(config)

    def exists(self, key: str) -> bool:
        try:
            self.client.stat_object(self.config.bucket, key)
            return True
        except Exception as error:
            if _is_not_found(error):
                return False
            raise

    def size(self, key: str) -> int | None:
        try:
            metadata = self.client.stat_object(self.config.bucket, key)
        except Exception as error:
            if _is_not_found(error):
                raise FileNotFoundError(key) from error
            raise
        size = getattr(metadata, "size", None)
        return size if isinstance(size, int) and size >= 0 else None

    def upload_file(self, key: str, path: Path) -> None:
        self.client.fput_object(
            self.config.bucket,
            key,
            str(path),
            content_type="application/zstd",
            part_size=MULTIPART_PART_SIZE,
            num_parallel_uploads=self.config.upload_concurrency,
        )

    def put_bytes(self, key: str, data: bytes) -> None:
        self.client.put_object(
            self.config.bucket,
            key,
            io.BytesIO(data),
            len(data),
            content_type="application/json",
        )

    def read_bytes(self, key: str, limit: int = METADATA_SIZE_LIMIT) -> bytes:
        try:
            response = self.client.get_object(self.config.bucket, key)
        except Exception as error:
            if _is_not_found(error):
                raise FileNotFoundError(key) from error
            raise
        chunks = bytearray()
        try:
            while len(chunks) <= limit:
                chunk = response.read(min(STREAM_READ_SIZE, limit + 1 - len(chunks)))
                if not chunk:
                    break
                chunks.extend(chunk)
            if len(chunks) > limit:
                raise ValueError(f"remote metadata exceeds {limit} bytes")
            return bytes(chunks)
        finally:
            _close_response(response)

    def open(self, key: str) -> BinaryIO:
        try:
            return self.client.get_object(self.config.bucket, key)
        except Exception as error:
            if _is_not_found(error):
                raise FileNotFoundError(key) from error
            raise

    def close(self, response: BinaryIO) -> None:
        _close_response(response)

    def list(self, prefix: str) -> Iterator[str]:
        for item in self.client.list_objects(
            self.config.bucket,
            prefix=prefix,
            recursive=True,
        ):
            key = getattr(item, "object_name", None)
            if isinstance(key, str):
                yield key
