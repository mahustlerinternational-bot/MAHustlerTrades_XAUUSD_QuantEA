"""Fail-closed capability guard for pre-v4 command-capable components."""

from __future__ import annotations

import os


BROKER_MODES = frozenset({"live", "demo", "shadow"})


def require_non_broker_mode(component: str) -> None:
    """Reject legacy components whenever a broker-capable mode is selected."""

    mode = os.environ.get("QSX_MODE", "").strip().casefold()
    if mode in BROKER_MODES:
        raise RuntimeError(
            f"{component} is a legacy pre-v4 component and is disabled in "
            f"{mode!r} mode"
        )


__all__ = ["BROKER_MODES", "require_non_broker_mode"]
