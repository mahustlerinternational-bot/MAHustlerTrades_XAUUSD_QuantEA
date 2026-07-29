#!/usr/bin/env python3
"""Retired one-shot source rewriter.

The original script rewrote runtime modules and recreated source-tree database
directories at import time.  It is intentionally retained only as a tombstone
so old operator notes fail safely instead of undoing the canonical migration.
"""


def main() -> int:
    raise SystemExit(
        "APPLY_FIX.py is retired: its source-tree path rewrites are unsafe and "
        "must not be run against the canonical v4 codebase."
    )


if __name__ == "__main__":
    main()
