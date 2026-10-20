"""Mode-isolated runtime paths and an exclusive process lock.

Path resolution is pure.  Directories and lock files are created only through
explicit method calls, so importing this module cannot touch live state.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

try:  # Package and legacy flat-import compatibility.
    from .runtime_config import RuntimeMode
except ImportError:  # pragma: no cover - exercised by legacy launch paths
    from runtime_config import RuntimeMode  # type: ignore


_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_SAFE_DB_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


class RuntimePathError(ValueError):
    """A runtime path is unsafe, ambiguous, or outside its managed root."""


class ProcessLockError(RuntimeError):
    """Another trading runtime already owns the requested process lock."""


@dataclass(frozen=True, slots=True)
class RuntimePaths:
    """All writable locations for one mode-isolated runtime instance."""

    data_root: Path
    mode: RuntimeMode
    instance_id: str
    instance_root: Path
    database_dir: Path
    state_dir: Path
    log_dir: Path
    lock_dir: Path

    @classmethod
    def resolve(
        cls,
        mode: RuntimeMode | str,
        *,
        data_dir: str | Path | None = None,
        instance_id: str = "default",
        environ: Mapping[str, str] | None = None,
    ) -> "RuntimePaths":
        """Resolve paths without creating or modifying anything."""

        try:
            runtime_mode = mode if isinstance(mode, RuntimeMode) else RuntimeMode(mode)
        except ValueError as exc:
            raise RuntimePathError(f"unknown runtime mode: {mode!r}") from exc
        _validate_component(instance_id, "instance_id")

        env = os.environ if environ is None else environ
        selected = data_dir
        if selected is None:
            selected = env.get("QSX_DATA_DIR")
        if selected is None:
            xdg_state = env.get("XDG_STATE_HOME")
            if xdg_state:
                selected = Path(xdg_state) / "quant_swarm_xau"
            else:
                selected = Path.home() / ".local" / "state" / "quant_swarm_xau"
        if isinstance(selected, str) and not selected.strip():
            raise RuntimePathError("runtime data directory cannot be empty")

        root = Path(selected).expanduser().resolve(strict=False)
        _validate_data_root(root)
        instance_root = root / runtime_mode.value / instance_id
        return cls(
            data_root=root,
            mode=runtime_mode,
            instance_id=instance_id,
            instance_root=instance_root,
            database_dir=instance_root / "databases",
            state_dir=instance_root / "state",
            log_dir=instance_root / "logs",
            lock_dir=instance_root / "locks",
        )

    @property
    def trade_ledger_path(self) -> Path:
        return self.database_path("trade_ledger")

    def ensure_layout(self) -> "RuntimePaths":
        """Create the standard private directory layout explicitly."""

        for directory in (
            self.instance_root,
            self.database_dir,
            self.state_dir,
            self.log_dir,
            self.lock_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                directory.chmod(0o700)
            except OSError as exc:
                raise RuntimePathError(
                    f"cannot secure runtime directory {directory}: {exc}"
                ) from exc
        return self

    def state_subdir(self, name: str, *, create: bool = False) -> Path:
        """Return one safe state subdirectory for routing legacy modules later."""

        _validate_component(name, "state subdirectory")
        path = self.state_dir / name
        if create:
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                path.chmod(0o700)
            except OSError as exc:
                raise RuntimePathError(f"cannot secure state directory {path}: {exc}") from exc
        return path

    def database_path(
        self,
        name: str,
        *,
        namespace: str | None = None,
        create_parent: bool = False,
    ) -> Path:
        """Return a safe ``.db`` path, optionally under a state namespace.

        Examples::

            paths.database_path("trade_ledger")
            paths.database_path("risk_ledger.db", namespace="legacy")
        """

        if not isinstance(name, str) or not _SAFE_DB_NAME.fullmatch(name):
            raise RuntimePathError("database name must be one safe filename component")
        filename = name if name.endswith(".db") else f"{name}.db"
        parent = self.database_dir if namespace is None else self.state_subdir(namespace)
        if create_parent:
            parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            try:
                parent.chmod(0o700)
            except OSError as exc:
                raise RuntimePathError(f"cannot secure database directory {parent}: {exc}") from exc
        return parent / filename

    def process_lock(self, name: str = "runtime") -> "ProcessLock":
        """Construct, but do not yet acquire, this instance's process lock."""

        _validate_component(name, "lock name")
        return ProcessLock(
            self.lock_dir / f"{name}.lock",
            metadata={
                "mode": self.mode.value,
                "instance_id": self.instance_id,
                "instance_root": str(self.instance_root),
            },
        )

    def ownership_lock(
        self,
        *,
        account_id: str,
        broker_server: str,
        symbol: str,
        magic: int,
    ) -> "ProcessLock":
        """Construct one cross-mode lock for an exact broker ownership tuple.

        This lock lives directly below ``data_root`` rather than an instance
        directory, so different modes and instance IDs cannot simultaneously
        own the same account/server/symbol/magic tuple.
        """

        identity: dict[str, str | int] = {
            "account_id": _required_identity(account_id, "account_id"),
            "broker_server": _required_identity(
                broker_server, "broker_server"
            ),
            "symbol": _required_identity(symbol, "symbol"),
            "magic": _positive_magic(magic),
        }
        canonical = json.dumps(
            identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        digest = hashlib.sha256(b"quant-swarm-ownership-v1\0" + canonical).hexdigest()
        return ProcessLock(
            self.data_root / "ownership" / f"{digest}.lock",
            metadata={"ownership": identity, "ownership_hash": digest},
        )

    def assert_managed_path(self, path: str | Path, *, allow_instance_root: bool = False) -> Path:
        """Reject traversal or accidental operations outside this instance root."""

        candidate = Path(path).expanduser().resolve(strict=False)
        root = self.instance_root.resolve(strict=False)
        if candidate == root:
            if allow_instance_root:
                return candidate
            raise RuntimePathError("operation on the entire instance root is not allowed")
        if not candidate.is_relative_to(root):
            raise RuntimePathError(f"path is outside managed runtime root: {candidate}")
        return candidate

    def assert_cleanup_allowed(self, path: str | Path) -> Path:
        """Allow cleanup only in simulation, replay, or test instance subpaths."""

        if self.mode in {RuntimeMode.DEMO, RuntimeMode.SHADOW}:
            raise RuntimePathError(f"cleanup is forbidden in {self.mode.value} mode")
        return self.assert_managed_path(path)


class ProcessLock:
    """Advisory, exclusive, non-replacing process lock backed by ``flock``.

    The lock file is deliberately not unlinked on release.  Keeping a stable
    inode avoids the classic race where one process locks an unlinked file while
    another creates and locks a replacement path.
    """

    def __init__(self, path: str | Path, *, metadata: Mapping[str, Any] | None = None):
        self.path = Path(path)
        self.metadata = dict(metadata or {})
        self._fd: int | None = None

    @property
    def acquired(self) -> bool:
        return self._fd is not None

    def acquire(self) -> "ProcessLock":
        if self._fd is not None:
            raise ProcessLockError(f"process lock is already acquired: {self.path}")
        try:
            import fcntl
        except ImportError as exc:  # pragma: no cover - current deployment is POSIX
            raise ProcessLockError("exclusive process locking requires POSIX flock") from exc

        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            self.path.parent.chmod(0o700)
        except OSError as exc:
            raise ProcessLockError(
                f"cannot secure process-lock directory {self.path.parent}: {exc}"
            ) from exc
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.chmod(self.path, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                holder = _read_lock_holder(fd)
                detail = f"; holder={holder}" if holder else ""
                raise ProcessLockError(
                    f"another runtime owns process lock {self.path}{detail}"
                ) from exc

            record = {
                **self.metadata,
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "acquired_at": time.time(),
            }
            payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, payload)
            os.fsync(fd)
            self._fd = fd
            return self
        except Exception:
            os.close(fd)
            raise

    def release(self) -> None:
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            import fcntl

            os.ftruncate(fd, 0)
            os.fsync(fd)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def __enter__(self) -> "ProcessLock":
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()


def _read_lock_holder(fd: int) -> str:
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        raw = os.read(fd, 16_384).decode("utf-8", errors="replace").strip()
        if not raw:
            return ""
        parsed = json.loads(raw)
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"))
    except (OSError, ValueError):
        return "unreadable"


def _validate_component(value: str, label: str) -> None:
    if not isinstance(value, str) or not _SAFE_COMPONENT.fullmatch(value):
        raise RuntimePathError(f"{label} must be one safe 1-64 character component")


def _required_identity(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimePathError(f"{label} must be a non-empty broker identity")
    normalized = value.strip()
    if len(normalized) > 128 or any(ord(character) < 32 for character in normalized):
        raise RuntimePathError(f"{label} contains unsupported characters")
    return normalized


def _positive_magic(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimePathError("magic must be a positive integer")
    return value


def _validate_data_root(path: Path) -> None:
    if not path.is_absolute():
        raise RuntimePathError("runtime data root must resolve to an absolute path")
    if path == Path(path.anchor):
        raise RuntimePathError("filesystem root cannot be used as runtime data root")
    try:
        home = Path.home().resolve(strict=False)
    except RuntimeError:
        home = None
    if home is not None and path == home:
        raise RuntimePathError("home directory itself cannot be used as runtime data root")


__all__ = [
    "ProcessLock",
    "ProcessLockError",
    "RuntimePathError",
    "RuntimePaths",
]
