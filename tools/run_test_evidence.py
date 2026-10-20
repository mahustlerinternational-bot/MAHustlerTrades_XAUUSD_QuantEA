#!/usr/bin/env python3
"""Run a canonical test lane in a networkless sandbox and sign its evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from pathlib import PurePosixPath

try:
    from .dependency_common import (
        LANES,
        DependencyGateError,
        assert_canonical_ssh_signature,
        canonical_name,
        parse_lock,
        sha256_bytes,
        snapshot_path,
    )
except ImportError:  # Script execution places tools/ directly on sys.path.
    from dependency_common import (  # type: ignore
        LANES,
        DependencyGateError,
        assert_canonical_ssh_signature,
        canonical_name,
        parse_lock,
        sha256_bytes,
        snapshot_path,
    )


CONTRACT_FIELDS = frozenset(
    {
        "allowed_skips",
        "expected_collected",
        "nodeids_sha256",
        "pytest_arguments",
        "required_nodeids",
        "schema_version",
    }
)
EVIDENCE_FIELDS = frozenset(
    {
        "artifact_class",
        "environment_sha256",
        "gate_exit_code",
        "installed_distributions",
        "interpreter",
        "lane",
        "lock_sha256",
        "pytest_arguments",
        "pytest_log_sha256",
        "pytest_nodeids_sha256",
        "pytest_result",
        "sandbox_profile_sha256",
        "schema_version",
        "signing_identity",
        "site_packages_sha256",
        "source_commit",
        "test_contract_sha256",
        "wheelhouse_manifest_sha256",
    }
)
INTERPRETER_FIELDS = frozenset(
    {
        "abi",
        "base_executable_sha256",
        "cache_tag",
        "implementation",
        "implementation_name",
        "machine",
        "pip_version",
        "platform",
        "python_executable_sha256",
        "soabi",
        "version",
    }
)
RESULT_FIELDS = frozenset(
    {
        "collected",
        "deselected",
        "errors",
        "exit_status",
        "failed",
        "nodeids_sha256",
        "passed",
        "schema_version",
        "skipped",
        "warnings",
        "xfailed",
        "xpassed",
    }
)
HEX_256 = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
IDENTITY = re.compile(r"^[A-Za-z0-9_.@-]{3,128}$")
CANONICAL_TEST_CONTRACT = PurePosixPath("release/test-evidence-contract.json")
SANDBOX_PROFILE = {
    "bubblewrap": "unshare-all",
    "filesystem": "host-root-read-only; output-and-tmp-only-write",
    "network": "isolated-network-namespace",
    "pytest_plugin_autoload": False,
    "version": 1,
}
PROBE = r"""
import json, platform, site, sys, sysconfig
print(json.dumps({
    "base_executable": sys._base_executable,
    "base_prefix": sys.base_prefix,
    "cache_tag": sys.implementation.cache_tag,
    "implementation": platform.python_implementation(),
    "implementation_name": sys.implementation.name,
    "machine": platform.machine(),
    "prefix": sys.prefix,
    "site_packages": site.getsitepackages(),
    "soabi": sysconfig.get_config_var("SOABI"),
    "version": platform.python_version(),
}, sort_keys=True))
"""


class TestEvidenceError(RuntimeError):
    """The test lane or its evidence violated the frozen release contract."""


def canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_env() -> dict[str, str]:
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    environment.update(
        {
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "LC_ALL": "C",
            "TZ": "UTC",
        }
    )
    return environment


def _git(
    root: Path, *arguments: str, binary: bool = False
) -> str | bytes:
    completed = subprocess.run(
        [
            "git",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.untrackedCache=false",
            "-C",
            os.fspath(root),
            *arguments,
        ],
        check=False,
        capture_output=True,
        text=not binary,
        env=_git_env(),
    )
    if completed.returncode != 0:
        detail = completed.stderr
        if isinstance(detail, bytes):
            detail = detail.decode("utf-8", errors="replace")
        raise TestEvidenceError(detail.strip() or "Git command failed")
    return completed.stdout if binary else completed.stdout.strip()


def _clean_commit(root: Path) -> str:
    root = root.resolve(strict=True)
    top = Path(str(_git(root, "rev-parse", "--show-toplevel"))).resolve(strict=True)
    if top != root:
        raise TestEvidenceError("source root must be the Git worktree top level")
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise TestEvidenceError("source worktree is dirty or contains untracked files")
    commit = str(_git(root, "rev-parse", "HEAD^{commit}"))
    if GIT_COMMIT.fullmatch(commit) is None:
        raise TestEvidenceError("source commit is invalid")
    return commit


def _load_json(
    path: Path,
    fields: frozenset[str],
    label: str,
) -> tuple[dict[str, object], bytes]:
    value, _ = snapshot_path(path, label=label, maximum_size=8 * 1024 * 1024)
    try:
        parsed = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TestEvidenceError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(parsed, dict) or frozenset(parsed) != fields:
        raise TestEvidenceError(f"{label} has missing or unknown fields")
    if canonical_json(parsed) != value:
        raise TestEvidenceError(f"{label} is not canonical JSON")
    return parsed, value


def _validate_contract(contract: dict[str, object]) -> None:
    if contract.get("schema_version") != 1:
        raise TestEvidenceError("test contract schema is unsupported")
    collected = contract.get("expected_collected")
    if type(collected) is not int or collected < 1:
        raise TestEvidenceError("test collection count is invalid")
    if HEX_256.fullmatch(str(contract.get("nodeids_sha256", ""))) is None:
        raise TestEvidenceError("test nodeid digest is invalid")
    for field in ("allowed_skips", "required_nodeids", "pytest_arguments"):
        values = contract.get(field)
        if (
            not isinstance(values, list)
            or not all(isinstance(value, str) and value for value in values)
            or len(values) != len(set(values))
        ):
            raise TestEvidenceError(f"test contract {field} is invalid")
    if contract["allowed_skips"] != sorted(contract["allowed_skips"]):  # type: ignore[arg-type]
        raise TestEvidenceError("allowed skips must be sorted")
    if contract["required_nodeids"] != sorted(contract["required_nodeids"]):  # type: ignore[arg-type]
        raise TestEvidenceError("required nodeids must be sorted")
    if contract["pytest_arguments"] != [
        "-q",
        "-W",
        "error",
        "-p",
        "no:cacheprovider",
    ]:
        raise TestEvidenceError("pytest arguments differ from the frozen lane")


def _load_committed_test_contract(
    source_root: Path, requested_path: Path, commit: str
) -> tuple[dict[str, object], bytes]:
    canonical_path = source_root / CANONICAL_TEST_CONTRACT.as_posix()
    requested = requested_path.expanduser().absolute()
    if (
        requested.resolve(strict=True) != canonical_path.resolve(strict=True)
        or requested != canonical_path
    ):
        raise TestEvidenceError(
            "test evidence contract must be the canonical source-tree file"
        )
    metadata = canonical_path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise TestEvidenceError("canonical test evidence contract is unsafe")
    contract, contract_value = _load_json(
        canonical_path,
        CONTRACT_FIELDS,
        "test evidence contract",
    )
    committed = _git(
        source_root,
        "cat-file",
        "blob",
        f"{commit}:{CANONICAL_TEST_CONTRACT.as_posix()}",
        binary=True,
    )
    assert isinstance(committed, bytes)
    if committed != contract_value:
        raise TestEvidenceError("test evidence contract differs from Git HEAD")
    return contract, contract_value


def _probe_interpreter(
    python: Path, lane_name: str
) -> tuple[dict[str, object], list[Path]]:
    lane = LANES[lane_name]
    invocation = python.expanduser().absolute()
    if not invocation.exists():
        raise TestEvidenceError("test interpreter invocation path is missing")
    resolved = invocation.resolve(strict=True)
    metadata = resolved.stat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid not in {0, os.getuid()}
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        raise TestEvidenceError("test interpreter is not a trusted regular file")
    completed = subprocess.run(
        [os.fspath(invocation), "-c", PROBE],
        check=False,
        capture_output=True,
        text=True,
        env={
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "PYTHONNOUSERSITE": "1",
            "TZ": "UTC",
        },
    )
    if completed.returncode != 0:
        raise TestEvidenceError("test interpreter probe failed")
    try:
        probe = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise TestEvidenceError("test interpreter probe is invalid") from exc
    expected_cache_tag = f"cpython-{lane.python_version_short.replace('.', '')}"
    if (
        probe.get("implementation") != lane.implementation
        or probe.get("implementation_name") != lane.implementation_name
        or probe.get("machine") != lane.machine
        or probe.get("version") != lane.python_version
        or probe.get("cache_tag") != expected_cache_tag
        or probe.get("prefix") == probe.get("base_prefix")
    ):
        raise TestEvidenceError("test interpreter does not match the frozen venv lane")
    site_packages = probe.get("site_packages")
    if (
        not isinstance(site_packages, list)
        or len(site_packages) != 1
        or not isinstance(site_packages[0], str)
    ):
        raise TestEvidenceError("test interpreter site-packages layout is invalid")
    base = Path(str(probe.get("base_executable", ""))).resolve(strict=True)
    pip = subprocess.run(
        [os.fspath(invocation), "-m", "pip", "--version"],
        check=False,
        capture_output=True,
        text=True,
        env={
            "LC_ALL": "C",
            "PATH": "/usr/bin:/bin",
            "PYTHONNOUSERSITE": "1",
            "TZ": "UTC",
        },
    )
    match = re.match(r"pip ([A-Za-z0-9_.+-]+) ", pip.stdout)
    if (
        pip.returncode != 0
        or match is None
        or match.group(1) != lane.pip_version
    ):
        raise TestEvidenceError("cannot inspect test-lane pip")
    interpreter = {
        "abi": lane.abi,
        "base_executable_sha256": sha256_file(base),
        "cache_tag": probe["cache_tag"],
        "implementation": probe["implementation"],
        "implementation_name": probe["implementation_name"],
        "machine": probe["machine"],
        "pip_version": match.group(1),
        "platform": lane.platform,
        "python_executable_sha256": sha256_file(resolved),
        "soabi": probe["soabi"],
        "version": probe["version"],
    }
    if frozenset(interpreter) != INTERPRETER_FIELDS:
        raise TestEvidenceError("interpreter evidence schema is inconsistent")
    return interpreter, [Path(site_packages[0]).resolve(strict=True)]


def _installed_distributions(
    python: Path, lock_value: bytes
) -> list[dict[str, str]]:
    expected = parse_lock(lock_value)
    invocation = python.expanduser().absolute()
    completed = subprocess.run(
        [
            os.fspath(invocation),
            "-m",
            "pip",
            "list",
            "--format=json",
            "--disable-pip-version-check",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "PYTHONNOUSERSITE": "1",
            "TZ": "UTC",
        },
    )
    try:
        records = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise TestEvidenceError("pip list did not return JSON") from exc
    if completed.returncode != 0 or not isinstance(records, list):
        raise TestEvidenceError("cannot enumerate installed distributions")
    installed: dict[str, str] = {}
    for record in records:
        if not isinstance(record, dict):
            raise TestEvidenceError("pip list record is invalid")
        name = canonical_name(str(record.get("name", "")))
        version = str(record.get("version", ""))
        if name == "pip":
            continue
        if name in installed:
            raise TestEvidenceError("installed distribution is duplicated")
        installed[name] = version
    expected_versions = {name: value[0] for name, value in expected.items()}
    if installed != expected_versions:
        raise TestEvidenceError("installed distributions differ from the exact test lock")
    check = subprocess.run(
        [os.fspath(invocation), "-m", "pip", "check"],
        check=False,
        capture_output=True,
        text=True,
        env={
            "LC_ALL": "C",
            "PATH": "/usr/bin:/bin",
            "PYTHONNOUSERSITE": "1",
            "TZ": "UTC",
        },
    )
    if check.returncode != 0:
        raise TestEvidenceError("pip check failed in the test lane")
    return [
        {"name": name, "version": installed[name]} for name in sorted(installed)
    ]


def _tree_hash(roots: list[Path]) -> str:
    records: list[tuple[str, int, int, str]] = []
    for root in roots:
        if root.is_symlink() or not root.is_dir():
            raise TestEvidenceError("site-packages root is unsafe")
        for candidate in (root, *sorted(root.rglob("*"))):
            metadata = candidate.lstat()
            relative = (
                f"{root.name}/."
                if candidate == root
                else f"{root.name}/{candidate.relative_to(root).as_posix()}"
            )
            mode = stat.S_IMODE(metadata.st_mode)
            if stat.S_ISDIR(metadata.st_mode):
                records.append((relative, mode, 0, "dir"))
            elif stat.S_ISREG(metadata.st_mode):
                records.append(
                    (relative, mode, metadata.st_size, sha256_file(candidate))
                )
            else:
                raise TestEvidenceError("site-packages contains a link or special file")
    return sha256_bytes(
        json.dumps(records, separators=(",", ":"), ensure_ascii=True).encode()
    )


def _validate_wheelhouse(
    *,
    source_root: Path,
    wheelhouse: Path,
    lane: str,
    allowed_signers: Path,
    signing_identity: str,
    commit: str,
) -> bytes:
    manifest_before, _ = snapshot_path(
        wheelhouse / "WHEELHOUSE.json",
        label="wheelhouse manifest before validation",
        maximum_size=4 * 1024 * 1024,
    )
    signature_before, _ = snapshot_path(
        wheelhouse / "WHEELHOUSE.json.sig",
        label="wheelhouse signature before validation",
        maximum_size=1024 * 1024,
    )
    completed = subprocess.run(
        [
            os.fspath(sys.executable),
            os.fspath(source_root / "tools" / "validate_wheelhouse.py"),
            "--source-root",
            os.fspath(source_root),
            "--wheelhouse",
            os.fspath(wheelhouse),
            "--lane",
            lane,
            "--allowed-signers",
            os.fspath(allowed_signers),
            "--signing-identity",
            signing_identity,
            "--expected-source-commit",
            commit,
            "--require-source-inputs",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "PYTHONNOUSERSITE": "1",
            "TZ": "UTC",
        },
    )
    if completed.returncode != 0:
        raise TestEvidenceError(
            completed.stderr.strip() or "signed wheelhouse validation failed"
        )
    manifest_after, _ = snapshot_path(
        wheelhouse / "WHEELHOUSE.json",
        label="validated wheelhouse manifest",
        maximum_size=4 * 1024 * 1024,
    )
    signature_after, _ = snapshot_path(
        wheelhouse / "WHEELHOUSE.json.sig",
        label="validated wheelhouse signature",
        maximum_size=1024 * 1024,
    )
    if manifest_after != manifest_before or signature_after != signature_before:
        raise TestEvidenceError("wheelhouse changed during signed validation")
    return manifest_after


def _run_pytest(
    *,
    source_root: Path,
    python: Path,
    output_dir: Path,
    arguments: list[str],
) -> tuple[dict[str, object], bytes, bytes]:
    result_path = output_dir / ".pytest-result.json"
    nodeids_path = output_dir / "PYTEST_NODEIDS.txt"
    home = output_dir / ".sandbox-home"
    home.mkdir(mode=0o700)
    environment = {
        "HOME": os.fspath(home),
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "QSX_PYTEST_EVIDENCE_NODEIDS": os.fspath(nodeids_path),
        "QSX_PYTEST_EVIDENCE_RESULT": os.fspath(result_path),
        "TZ": "UTC",
        "XDG_CACHE_HOME": os.fspath(home / ".cache"),
        "XDG_CONFIG_HOME": os.fspath(home / ".config"),
    }
    command = [
        os.fspath(python.expanduser().absolute()),
        "-m",
        "pytest",
        *arguments,
        "-p",
        "tools.pytest_evidence_plugin",
    ]
    sandbox = [
        "/usr/bin/bwrap",
        "--die-with-parent",
        "--new-session",
        "--unshare-all",
        "--ro-bind",
        "/",
        "/",
        "--dev",
        "/dev",
        "--proc",
        "/proc",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        "/run",
        "--bind",
        os.fspath(output_dir),
        os.fspath(output_dir),
        "--clearenv",
    ]
    for key, value in sorted(environment.items()):
        sandbox.extend(("--setenv", key, value))
    sandbox.extend(("--chdir", os.fspath(source_root), "--", *command))
    completed = subprocess.run(
        sandbox,
        check=False,
        capture_output=True,
        env={"LC_ALL": "C", "PATH": "/usr/bin:/bin", "TZ": "UTC"},
    )
    log = completed.stdout + b"\n[stderr]\n" + completed.stderr
    if completed.returncode != 0:
        raise TestEvidenceError(f"pytest returned {completed.returncode}")
    result, _ = _load_json(result_path, RESULT_FIELDS, "pytest result")
    if result.get("exit_status") != 0:
        raise TestEvidenceError("pytest plugin reported a failing exit status")
    nodeids, _ = snapshot_path(
        nodeids_path,
        label="pytest collected nodeids",
        maximum_size=8 * 1024 * 1024,
    )
    return result, log, nodeids


def _sign(path: Path, key: Path) -> None:
    key = key.expanduser().absolute()
    metadata = key.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) not in {0o400, 0o600}
    ):
        raise TestEvidenceError("test evidence signing key is unsafe")
    completed = subprocess.run(
        [
            "ssh-keygen",
            "-Y",
            "sign",
            "-f",
            os.fspath(key),
            "-n",
            "file",
            os.fspath(path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env={"LC_ALL": "C", "PATH": "/usr/bin:/bin", "TZ": "UTC"},
    )
    signature = path.with_suffix(path.suffix + ".sig")
    if completed.returncode != 0 or not signature.is_file() or signature.is_symlink():
        raise TestEvidenceError("test evidence signing failed")
    signature.chmod(0o400)


def run(arguments: argparse.Namespace) -> Path:
    if os.geteuid() == 0:
        raise TestEvidenceError("test evidence runner must not run as root")
    if arguments.lane not in {"test-py312", "test-py314"}:
        raise TestEvidenceError("release tests require a test dependency lane")
    if IDENTITY.fullmatch(arguments.signing_identity) is None:
        raise TestEvidenceError("test evidence signing identity is invalid")
    source_root = arguments.source_root.expanduser().resolve(strict=True)
    commit = _clean_commit(source_root)
    contract, contract_value = _load_committed_test_contract(
        source_root,
        arguments.test_contract,
        commit,
    )
    _validate_contract(contract)
    wheelhouse_manifest = _validate_wheelhouse(
        source_root=source_root,
        wheelhouse=arguments.wheelhouse.expanduser().absolute(),
        lane=arguments.lane,
        allowed_signers=arguments.allowed_signers.expanduser().absolute(),
        signing_identity=arguments.signing_identity,
        commit=commit,
    )
    lane = LANES[arguments.lane]
    lock_value, _ = snapshot_path(
        source_root / lane.lock_path,
        label="canonical test lock",
        maximum_size=8 * 1024 * 1024,
    )
    interpreter, site_packages = _probe_interpreter(arguments.python, arguments.lane)
    installed = _installed_distributions(arguments.python, lock_value)
    site_before = _tree_hash(site_packages)

    output_dir = arguments.output_dir.expanduser().absolute()
    if output_dir.exists() or output_dir.is_symlink():
        raise TestEvidenceError("test evidence output must not already exist")
    try:
        output_dir.parent.resolve(strict=True).relative_to(Path("/var/tmp"))
    except (OSError, ValueError) as exc:
        raise TestEvidenceError("test evidence output must be below /var/tmp") from exc
    output_dir.mkdir(mode=0o700)
    result, log, nodeids_value = _run_pytest(
        source_root=source_root,
        python=arguments.python,
        output_dir=output_dir,
        arguments=contract["pytest_arguments"],  # type: ignore[arg-type]
    )
    try:
        nodeids = nodeids_value.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise TestEvidenceError("pytest nodeids are not UTF-8") from exc
    allowed_skips = contract["allowed_skips"]
    required = contract["required_nodeids"]
    if (
        result["collected"] != contract["expected_collected"]
        or result["nodeids_sha256"] != contract["nodeids_sha256"]
        or sha256_bytes(nodeids_value) != contract["nodeids_sha256"]
        or not set(required).issubset(nodeids)  # type: ignore[arg-type]
        or result["failed"] != 0
        or result["errors"] != 0
        or result["warnings"] != 0
        or result["deselected"] != 0
        or result["xfailed"]
        or result["xpassed"]
        or result["skipped"] != allowed_skips
        or result["passed"] + len(result["skipped"]) != result["collected"]  # type: ignore[arg-type]
    ):
        raise TestEvidenceError("pytest result differs from the frozen test contract")
    site_after = _tree_hash(site_packages)
    if site_before != site_after:
        raise TestEvidenceError("test execution changed the installed dependency tree")
    interpreter_after, site_packages_after = _probe_interpreter(
        arguments.python, arguments.lane
    )
    if (
        interpreter_after != interpreter
        or site_packages_after != site_packages
        or _installed_distributions(arguments.python, lock_value) != installed
    ):
        raise TestEvidenceError(
            "test interpreter or installed distribution closure changed during execution"
        )
    if _clean_commit(source_root) != commit:
        raise TestEvidenceError("test execution changed the source worktree")

    environment = {
        "LC_ALL": "C.UTF-8",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TZ": "UTC",
    }
    evidence = {
        "artifact_class": "canonical-test-evidence",
        "environment_sha256": sha256_bytes(canonical_json(environment)),
        "gate_exit_code": 0,
        "installed_distributions": installed,
        "interpreter": interpreter,
        "lane": arguments.lane,
        "lock_sha256": sha256_bytes(lock_value),
        "pytest_arguments": contract["pytest_arguments"],
        "pytest_log_sha256": sha256_bytes(log),
        "pytest_nodeids_sha256": sha256_bytes(nodeids_value),
        "pytest_result": result,
        "sandbox_profile_sha256": sha256_bytes(canonical_json(SANDBOX_PROFILE)),
        "schema_version": 1,
        "signing_identity": arguments.signing_identity,
        "site_packages_sha256": site_before,
        "source_commit": commit,
        "test_contract_sha256": sha256_bytes(contract_value),
        "wheelhouse_manifest_sha256": sha256_bytes(wheelhouse_manifest),
    }
    if frozenset(evidence) != EVIDENCE_FIELDS:
        raise TestEvidenceError("test evidence schema implementation is inconsistent")
    log_path = output_dir / "PYTEST.log"
    log_path.write_bytes(log)
    log_path.chmod(0o400)
    (output_dir / "PYTEST_NODEIDS.txt").chmod(0o400)
    evidence_path = output_dir / "TEST_EVIDENCE.json"
    evidence_path.write_bytes(canonical_json(evidence))
    evidence_path.chmod(0o400)
    _sign(evidence_path, arguments.signing_key)
    signature_value, _ = snapshot_path(
        evidence_path.with_suffix(evidence_path.suffix + ".sig"),
        label="generated test evidence signature",
        maximum_size=1024 * 1024,
    )
    assert_canonical_ssh_signature(
        signature_value, "generated test evidence signature"
    )
    shutil.rmtree(output_dir / ".sandbox-home")
    (output_dir / ".pytest-result.json").unlink()
    output_dir.chmod(0o500)
    return evidence_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--lane", required=True, choices=("test-py312", "test-py314"))
    parser.add_argument("--python", required=True, type=Path)
    parser.add_argument("--wheelhouse", required=True, type=Path)
    parser.add_argument("--allowed-signers", required=True, type=Path)
    parser.add_argument("--signing-key", required=True, type=Path)
    parser.add_argument("--signing-identity", required=True)
    parser.add_argument("--test-contract", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    arguments = parser.parse_args(argv)
    try:
        result = run(arguments)
    except (
        DependencyGateError,
        OSError,
        subprocess.SubprocessError,
        TestEvidenceError,
    ) as exc:
        print(f"TEST_EVIDENCE_FAILED {exc}", file=sys.stderr)
        return 1
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
