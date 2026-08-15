from __future__ import annotations

import hashlib
import re
from pathlib import Path

from .config import NAMESPACE_MAX_LENGTH


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-") or "root"
    slug = re.sub(r"_+", "_", slug)
    if len(slug) > NAMESPACE_MAX_LENGTH:
        digest = hashlib.sha256(value.encode()).hexdigest()[:12]
        slug = f"{slug[:NAMESPACE_MAX_LENGTH - 13]}_{digest}"
    return slug


def local_namespace(root: Path) -> str:
    resolved = root.resolve()
    digest = hashlib.sha256(str(resolved).encode()).hexdigest()[:10]
    return _safe_slug(f"local_{resolved.name or 'root'}_{digest}")
