"""Compatibility entry point for agent executable shims."""

from .agents.shim import ensure_shims, main, run

__all__ = ["ensure_shims", "main", "run"]


if __name__ == "__main__":
    raise SystemExit(main())
