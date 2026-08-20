"""Identify the exact Memo source loaded into a long-running process."""

from __future__ import annotations

import hashlib
from pathlib import Path


def _runtime_id() -> str:
    package = Path(__file__).parent
    digest = hashlib.sha256()
    for path in sorted(package.rglob("*.py")):
        digest.update(path.relative_to(package).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


# Compute this once. An editable install may change on disk while the daemon is
# alive; retaining the import-time identity is what lets a new client notice.
RUNTIME_ID = _runtime_id()
