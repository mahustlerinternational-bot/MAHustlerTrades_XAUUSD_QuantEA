import os
import sys
import hashlib
import stat
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON_SWARM = ROOT / "python_swarm"
sys.path.insert(0, str(PYTHON_SWARM))
sys.path.insert(0, str(ROOT))

from safe_paths import create_ephemeral_workspace, guarded_rmtree


# This is established before pytest imports test modules, so module-level
# runtime paths resolve beneath /tmp rather than inside the source checkout.
TEST_STATE_ROOT = create_ephemeral_workspace("pytest")
os.environ["QSX_MODE"] = "test"
os.environ["QSX_STATE_DIR"] = str(TEST_STATE_ROOT)


PROTECTED_EVIDENCE_ROOTS = (
    ROOT / "python_swarm" / "journal_db_sim",
    ROOT / "RECOVERY_2026-07-15_OPEN_DB_HANDLES",
    ROOT / "logs",
    ROOT / "python_swarm" / "logs",
    ROOT / "backtest_results",
    ROOT / "python_swarm" / "backtest_results",
)


def _evidence_manifest() -> dict[str, tuple[str, int, int]]:
    """Fingerprint persistent evidence before any test module is imported."""

    manifest: dict[str, tuple[str, int, int]] = {}
    for evidence_root in PROTECTED_EVIDENCE_ROOTS:
        if not evidence_root.exists():
            continue
        for path in sorted(evidence_root.rglob("*")):
            relative = str(path.relative_to(ROOT))
            metadata = path.lstat()
            mode = stat.S_IMODE(metadata.st_mode)
            if path.is_symlink():
                manifest[relative] = (
                    f"symlink:{os.readlink(path)}",
                    metadata.st_size,
                    mode,
                )
            elif path.is_file():
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                manifest[relative] = (digest, metadata.st_size, mode)
            elif path.is_dir():
                manifest[f"{relative}/"] = ("directory", 0, mode)
    return manifest


_EVIDENCE_BEFORE = _evidence_manifest()


def pytest_sessionfinish(session, exitstatus):
    try:
        after = _evidence_manifest()
        if after != _EVIDENCE_BEFORE:
            before_keys = set(_EVIDENCE_BEFORE)
            after_keys = set(after)
            changed = sorted(
                key
                for key in before_keys & after_keys
                if _EVIDENCE_BEFORE[key] != after[key]
            )
            detail = {
                "created": sorted(after_keys - before_keys),
                "deleted": sorted(before_keys - after_keys),
                "changed": changed,
            }
            session.exitstatus = 1
            raise RuntimeError(
                "tests modified protected production/recovery evidence: "
                f"{detail}"
            )
    finally:
        guarded_rmtree(TEST_STATE_ROOT, TEST_STATE_ROOT)
