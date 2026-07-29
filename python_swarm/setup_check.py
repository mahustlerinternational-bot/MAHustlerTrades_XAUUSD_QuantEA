"""Canonical setup checks and immutable broker-deployment validation.

Importing this module performs no checks and changes no files.  ``CHECK`` mode
is intentionally usable from a developer checkout.  Broker-connected modes are
different: before they create runtime state or open a socket they must call
``assert_broker_deployment_integrity`` and run from a non-root service account
whose source, configuration, interpreter and import roots are not writable by
that account.

The integrity check never tries to repair permissions.  A writable development
checkout must be packaged into a protected release tree instead of being
modified in place.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import os
import shutil
import site
import stat
import sys
import sysconfig
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

try:  # Package import plus support for direct diagnostic invocation.
    from .runtime_config import ConfigError, RuntimeMode, load_runtime_config
except ImportError:  # pragma: no cover - direct ``python setup_check.py`` only
    from runtime_config import ConfigError, RuntimeMode, load_runtime_config  # type: ignore


CANONICAL_MODULES: tuple[str, ...] = (
    "python_swarm.agent_roster",
    "python_swarm.broker_gateway",
    "python_swarm.broker_protocol",
    "python_swarm.decision_service",
    "python_swarm.execution_service",
    "python_swarm.integration_dispatcher",
    "python_swarm.learning_service",
    "python_swarm.lifecycle_service",
    "python_swarm.market_snapshot_service",
    "python_swarm.provider_adapters",
    "python_swarm.reconciliation",
    "python_swarm.runtime_config",
    "python_swarm.runtime_paths",
    "python_swarm.runtime_state",
    "python_swarm.trade_ledger",
    "python_swarm.trading_policies",
)

REQUIRED_DEPENDENCIES: tuple[str, ...] = ("aiohttp", "numpy")
BROKER_MODES = frozenset({RuntimeMode.SHADOW, RuntimeMode.DEMO, RuntimeMode.LIVE})


class SetupCheckError(RuntimeError):
    """A required setup or configuration check failed."""


class DeploymentIntegrityError(SetupCheckError):
    """Broker mode is running from mutable or ambiguous executable material."""


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class SetupCheckReport:
    mode: RuntimeMode
    config_path: Path
    results: tuple[CheckResult, ...]

    @property
    def ok(self) -> bool:
        return all(result.ok for result in self.results)


def assert_broker_deployment_integrity(
    *,
    mode: RuntimeMode | str,
    config_path: str | Path,
    project_root: str | Path | None = None,
    python_executable: str | Path | None = None,
    module_search_paths: Iterable[str | Path] | None = None,
) -> tuple[Path, ...]:
    """Fail closed unless broker code and configuration are immutable to us.

    The returned tuple contains the normalized trusted paths that were checked.
    The function is read-only and raises :class:`DeploymentIntegrityError` on
    the first ambiguity.  Call it before directory creation, locks, ledger
    migration, provider polling, or gateway startup.
    """

    runtime_mode = _runtime_mode(mode)
    if runtime_mode not in BROKER_MODES:
        raise DeploymentIntegrityError(
            "immutable deployment validation is required only for broker-connected "
            f"modes, not {runtime_mode.value!r}"
        )
    if not hasattr(os, "geteuid") or not hasattr(os, "getegid"):
        raise DeploymentIntegrityError(
            "broker deployment integrity requires POSIX effective identity checks"
        )
    if os.geteuid() == 0:
        raise DeploymentIntegrityError("broker modes must not run as root")
    if not (3, 11) <= sys.version_info[:2] <= (3, 12):
        raise DeploymentIntegrityError(
            "broker modes require a supported Python 3.11-3.12 interpreter; "
            "the frozen reference platform is Python 3.12"
        )

    root = Path(project_root or Path(__file__).resolve().parents[1])
    config = Path(config_path)
    executable = _resolve_executable(python_executable or sys.executable)

    package_root = root / "python_swarm"
    trusted: list[tuple[str, Path, bool, str]] = [
        ("release root", root, False, "directory"),
        ("python package", package_root, False, "directory"),
        ("runtime config", config, False, "file"),
        ("interpreter invocation", executable, True, "file"),
    ]

    if package_root.is_dir():
        try:
            for source in package_root.rglob("*"):
                if source.is_symlink() or (
                    source.is_file()
                    and source.suffix.casefold() in {".py", ".pyc", ".so", ".pyd"}
                ):
                    trusted.append(("python module", source, False, "file"))
        except OSError as exc:
            raise DeploymentIntegrityError(
                f"cannot enumerate Python package {package_root}: {exc}"
            ) from exc

    if module_search_paths is None:
        search_paths = _default_module_search_paths(root)
    else:
        search_paths = tuple(Path(path) for path in module_search_paths)
    trusted.extend(("module search path", path, False, "path") for path in search_paths)

    checked: list[Path] = []
    seen: set[Path] = set()
    for label, candidate, allow_symlink, expected_type in trusted:
        for normalized in _trusted_path_variants(
            candidate,
            label=label,
            allow_final_symlink=allow_symlink,
            expected_type=expected_type,
        ):
            if normalized in seen:
                continue
            _assert_path_chain_not_writable(normalized, label=label)
            seen.add(normalized)
            checked.append(normalized)
    return tuple(checked)


def run_setup_check(
    *,
    mode: RuntimeMode | str,
    config_path: str | Path,
    environ: Mapping[str, str] | None = None,
    project_root: str | Path | None = None,
    python_executable: str | Path | None = None,
    verify_broker_integrity: bool = True,
) -> SetupCheckReport:
    """Validate one explicit configuration and the canonical import surface."""

    runtime_mode = _runtime_mode(mode)
    path = Path(config_path).expanduser()
    results: list[CheckResult] = []

    try:
        config = load_runtime_config(path, environ=environ)
        if config.mode is not runtime_mode:
            raise SetupCheckError(
                f"requested mode {runtime_mode.value!r} does not match "
                f"config.mode {config.mode.value!r}"
            )
    except (ConfigError, OSError, SetupCheckError) as exc:
        results.append(CheckResult("configuration", False, str(exc)))
    else:
        results.append(
            CheckResult(
                "configuration",
                True,
                f"schema={config.schema_version} mode={config.mode.value}",
            )
        )

    runtime_spec = importlib.util.find_spec("python_swarm.runtime_app")
    if runtime_spec is None or runtime_spec.origin is None:
        results.append(
            CheckResult("canonical runtime", False, "python_swarm.runtime_app not found")
        )
    else:
        results.append(CheckResult("canonical runtime", True, runtime_spec.origin))

    for module_name in (*CANONICAL_MODULES, *REQUIRED_DEPENDENCIES):
        try:
            imported = importlib.import_module(module_name)
        except Exception as exc:  # Import health must report arbitrary import failures.
            results.append(
                CheckResult(
                    f"import {module_name}",
                    False,
                    f"{type(exc).__name__}: {exc}",
                )
            )
        else:
            origin = getattr(imported, "__file__", None) or "built-in"
            results.append(CheckResult(f"import {module_name}", True, str(origin)))

    if runtime_mode in BROKER_MODES and verify_broker_integrity:
        try:
            checked = assert_broker_deployment_integrity(
                mode=runtime_mode,
                config_path=path,
                project_root=project_root,
                python_executable=python_executable,
            )
        except DeploymentIntegrityError as exc:
            results.append(CheckResult("immutable deployment", False, str(exc)))
        else:
            results.append(
                CheckResult(
                    "immutable deployment",
                    True,
                    f"verified {len(checked)} trusted paths",
                )
            )

    return SetupCheckReport(runtime_mode, path, tuple(results))


def _runtime_mode(mode: RuntimeMode | str) -> RuntimeMode:
    try:
        return mode if isinstance(mode, RuntimeMode) else RuntimeMode(str(mode).lower())
    except ValueError as exc:
        raise SetupCheckError(f"unsupported runtime mode: {mode!r}") from exc


def _resolve_executable(value: str | Path) -> Path:
    raw = os.fspath(value)
    if not raw:
        raise DeploymentIntegrityError("interpreter path is empty")
    if os.sep not in raw:
        found = shutil.which(raw)
        if found is None:
            raise DeploymentIntegrityError(f"interpreter is not executable: {raw}")
        raw = found
    candidate = Path(raw).expanduser().absolute()
    if not candidate.exists() or not candidate.is_file() or not os.access(candidate, os.X_OK):
        raise DeploymentIntegrityError(f"interpreter is not executable: {candidate}")
    return candidate


def _default_module_search_paths(project_root: Path) -> tuple[Path, ...]:
    candidates: list[Path] = [project_root]
    candidates.extend(Path(value) if value else Path.cwd() for value in sys.path)
    candidates.extend(
        Path(value)
        for key, value in sysconfig.get_paths().items()
        if key in {"stdlib", "platstdlib", "purelib", "platlib"} and value
    )
    try:
        candidates.extend(Path(value) for value in site.getsitepackages())
    except AttributeError:  # pragma: no cover - unusual embedded Python
        pass
    candidates.extend(
        Path(origin)
        for module in tuple(sys.modules.values())
        if (origin := getattr(module, "__file__", None))
    )

    unique: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        absolute = candidate.expanduser().absolute()
        while not absolute.exists() and absolute != absolute.parent:
            absolute = absolute.parent
        if not absolute.exists():  # pragma: no cover - every absolute path reaches root
            raise DeploymentIntegrityError(
                f"module search path has no verifiable ancestor: {candidate}"
            )
        normalized = absolute.resolve(strict=True)
        if normalized not in seen:
            seen.add(normalized)
            unique.append(absolute)
    return tuple(unique)


def _trusted_path_variants(
    path: Path,
    *,
    label: str,
    allow_final_symlink: bool,
    expected_type: str,
) -> tuple[Path, ...]:
    absolute = path.expanduser().absolute()
    if not absolute.exists():
        raise DeploymentIntegrityError(f"{label} does not exist: {absolute}")
    if absolute.is_symlink() and not allow_final_symlink:
        raise DeploymentIntegrityError(f"{label} must not be a symlink: {absolute}")
    if expected_type == "file" and not absolute.is_file():
        raise DeploymentIntegrityError(f"{label} must be a regular file: {absolute}")
    if expected_type == "directory" and not absolute.is_dir():
        raise DeploymentIntegrityError(f"{label} must be a directory: {absolute}")
    if expected_type == "path" and not (absolute.is_file() or absolute.is_dir()):
        raise DeploymentIntegrityError(
            f"{label} must be a regular file or directory: {absolute}"
        )
    try:
        resolved = absolute.resolve(strict=True)
    except OSError as exc:
        raise DeploymentIntegrityError(f"cannot resolve {label} {absolute}: {exc}") from exc
    return (absolute,) if resolved == absolute else (absolute, resolved)


def _assert_path_chain_not_writable(path: Path, *, label: str) -> None:
    chain = [path]
    chain.extend(path.parents)
    for candidate in chain:
        try:
            metadata = candidate.stat()
        except OSError as exc:
            raise DeploymentIntegrityError(
                f"cannot inspect permissions for {label} {candidate}: {exc}"
            ) from exc
        if _identity_has_write_bits(metadata):
            raise DeploymentIntegrityError(
                f"{label} grants write permission to the service identity: {candidate}"
            )
        try:
            writable = os.access(candidate, os.W_OK, effective_ids=True)
        except (NotImplementedError, TypeError):  # pragma: no cover - older POSIX Python
            writable = os.access(candidate, os.W_OK)
        except OSError as exc:
            raise DeploymentIntegrityError(
                f"cannot verify permissions for {label} {candidate}: {exc}"
            ) from exc
        if writable:
            raise DeploymentIntegrityError(
                f"{label} path is writable by the service identity: {candidate}"
            )


def _identity_has_write_bits(metadata: os.stat_result) -> bool:
    """Check Unix mode bits even when a read-only mount masks ``os.access``."""

    effective_uid = os.geteuid()
    effective_groups = {os.getegid(), *os.getgroups()}
    if metadata.st_uid == effective_uid:
        return bool(metadata.st_mode & stat.S_IWUSR)
    if metadata.st_gid in effective_groups:
        return bool(metadata.st_mode & stat.S_IWGRP)
    return bool(metadata.st_mode & stat.S_IWOTH)


def _parser() -> argparse.ArgumentParser:
    configured_default = (
        os.environ.get("QSX_MODE", "check").strip().lower() or "check"
    )
    if configured_default not in {"check", "shadow", "demo"}:
        configured_default = "check"
    parser = argparse.ArgumentParser(
        description="Validate canonical QUANT-SWARM-XAU configuration and imports."
    )
    parser.add_argument(
        "--mode",
        choices=("check", "shadow", "demo"),
        default=configured_default,
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("QSX_CONFIG"),
        help="schema-v2 JSON configuration (defaults to QSX_CONFIG)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not args.config:
        print("FAIL configuration: --config or QSX_CONFIG is required", file=sys.stderr)
        return 2
    try:
        report = run_setup_check(mode=args.mode, config_path=args.config)
    except SetupCheckError as exc:
        print(f"FAIL setup: {exc}", file=sys.stderr)
        return 2

    print(
        f"QUANT-SWARM-XAU setup check: mode={report.mode.value} "
        f"config={report.config_path}"
    )
    for result in report.results:
        marker = "OK" if result.ok else "FAIL"
        stream = sys.stdout if result.ok else sys.stderr
        print(f"{marker:4} {result.name}: {result.detail}", file=stream)
    if report.ok:
        print("PASS canonical preflight")
        return 0
    print("FAIL canonical preflight", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
