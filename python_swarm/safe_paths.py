"""Filesystem safety helpers for simulations, demos, and tests.

Runtime code must never recursively delete a configured live state directory.
Only workspaces created here, underneath the operating-system temporary
directory and carrying our marker, are eligible for automatic cleanup.
"""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile
from pathlib import Path


EPHEMERAL_MARKER = ".quant_swarm_ephemeral"
EPHEMERAL_PREFIX = "quant-swarm-"


def runtime_state_dir() -> Path:
    """Return the explicitly configured state root without creating it.

    Persistent state must never fall back to a directory inside the source
    checkout.  Launchers choose a private runtime root and direct demo entry
    points call :func:`bootstrap_ephemeral_state` before resolving paths.
    """
    configured = os.environ.get("QSX_STATE_DIR", "").strip()
    if not configured:
        raise RuntimeError(
            "QSX_STATE_DIR is required; use RuntimePaths for persistent runs "
            "or bootstrap_ephemeral_state() for a simulation"
        )
    return Path(configured).expanduser().resolve()


def create_ephemeral_workspace(label: str) -> Path:
    """Create and mark a private temporary workspace."""
    safe_label = "".join(c for c in label.lower() if c.isalnum() or c == "-") or "run"
    workspace = Path(
        tempfile.mkdtemp(prefix=f"{EPHEMERAL_PREFIX}{safe_label}-")
    ).resolve()
    (workspace / EPHEMERAL_MARKER).write_text("ephemeral\n", encoding="utf-8")
    return workspace


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def guarded_rmtree(target: Path | str, workspace_root: Path | str) -> None:
    """Delete *target* only when it belongs to a marked temp workspace.

    The explicit root, temporary-directory containment, and marker checks are
    deliberately redundant. A bad caller therefore fails closed instead of
    deleting persistent state.
    """
    target_path = Path(target).expanduser().resolve()
    root = Path(workspace_root).expanduser().resolve()
    temp_root = Path(tempfile.gettempdir()).resolve()

    if root == temp_root or not _is_relative_to(root, temp_root):
        raise RuntimeError(f"Refusing cleanup outside temporary storage: {root}")
    if not (root / EPHEMERAL_MARKER).is_file():
        raise RuntimeError(f"Refusing cleanup of unmarked workspace: {root}")
    if target_path != root and not _is_relative_to(target_path, root):
        raise RuntimeError(f"Refusing cleanup outside workspace: {target_path}")

    if target_path.exists():
        shutil.rmtree(target_path)


def bootstrap_ephemeral_state(label: str) -> Path | None:
    """Configure an ephemeral state root for a directly executed demo.

    A caller-supplied ``QSX_STATE_DIR`` is never registered for deletion.
    """
    if os.environ.get("QSX_STATE_DIR", "").strip():
        return None

    workspace = create_ephemeral_workspace(label)
    os.environ["QSX_MODE"] = "simulation"
    os.environ["QSX_STATE_DIR"] = str(workspace)
    atexit.register(guarded_rmtree, workspace, workspace)
    return workspace
