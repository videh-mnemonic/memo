"""Launch the long-running Memo daemon for ``python -m memo.daemon``."""

from .server import main

raise SystemExit(main())
